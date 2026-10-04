package store

// 本文件实现读取自愈与巡检修复。
//
// 语义：
//   - 读取时若首选副本损坏/缺失而同分片还有其它完好副本，读取照常返回
//     正确内容，并顺手把完好字节补写回该副本"早已确定的位置"（提交记录
//     中的固定路径），让副本数恢复；补写失败只记录、不影响本次读取；
//   - Scrub 对当前所有存活对象做一遍健康检查，报告每个副本
//     好/坏/缺失及所在位置，自动修复能修好的部分，返回可解释统计；
//   - 自愈与巡检都只写回已生效布局中的既定位置，绝不改变副本布局，
//     也不会让同一时刻的读取结果发生变化。

import (
	"fmt"
	"hash/crc32"
	"sort"
)

// ReplicaStatus 是单个副本位置的健康状态。
type ReplicaStatus string

const (
	ReplicaOK          ReplicaStatus = "ok"          // 字节存在且 CRC 正确
	ReplicaMissing     ReplicaStatus = "missing"     // 文件不存在
	ReplicaCorrupt     ReplicaStatus = "corrupt"     // 字节存在但 CRC 不符
	ReplicaUnavailable ReplicaStatus = "unavailable" // 所在后端当前不可用
)

// ReplicaHealth 描述对一个副本位置的检查/修复结论。
type ReplicaHealth struct {
	ObjectID string
	Block    int
	Shard    int
	Replica  int
	Backend  string
	Path     string
	Status   ReplicaStatus
	Repaired bool   // 本轮是否被成功补写修复
	Detail   string // 判定依据
}

// RepairEvent 记录一次自愈/巡检补写尝试（成功或失败都留痕）。
type RepairEvent struct {
	ObjectID string
	Block    int
	Shard    int
	Replica  int
	Backend  string
	Path     string
	OK       bool   // 补写是否成功
	Trigger  string // "read" 或 "scrub"
	Detail   string
}

// ScrubReport 是一次巡检的可解释统计。
type ScrubReport struct {
	CheckedObjects  int
	CheckedReplicas int
	OK              int // 检查即完好的副本数
	Repaired        int // 本轮成功修复的副本数
	RepairFailed    int // 尝试修复但失败的副本数
	Degraded        int // 修复后仍无任何完好副本的分片数
	Findings        []ReplicaHealth
	Reason          string
}

// recordRepair 追加一条修复事件（读自愈与巡检共用）。
func (s *Store) recordRepair(ev RepairEvent) {
	s.mu.Lock()
	s.repairs = append(s.repairs, ev)
	s.mu.Unlock()
}

// RepairEvents 返回迄今为止记录的全部修复事件（按发生顺序）。
func (s *Store) RepairEvents() []RepairEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]RepairEvent(nil), s.repairs...)
}

// failedLoc 是读路径上发现的一个失败副本位置。
type failedLoc struct {
	replica int
	backend string
	path    string
	err     error
}

// healReplicas 把完好分片字节补写回读路径上失败的位置（尽力而为）。
// 只写提交记录中既定的 (backend, path)，绝不改变布局；
// 任何失败只记录事件，不影响读取结果。
func (s *Store) healReplicas(rec CommitRecord, shard int, failed []failedLoc, data []byte) {
	for _, f := range failed {
		ev := RepairEvent{
			ObjectID: rec.ObjectID, Block: rec.Block, Shard: shard,
			Replica: f.replica, Backend: f.backend, Path: f.path,
			Trigger: "read", Detail: fmt.Sprintf("read observed: %v", f.err),
		}
		back := s.backends[f.backend]
		switch {
		case back == nil || !back.Available():
			ev.Detail += "; repair skipped: backend unavailable"
		default:
			if err := back.Write(f.path, data); err != nil {
				ev.Detail += "; repair failed: " + err.Error()
			} else {
				ev.OK = true
				ev.Detail += "; repaired in place"
			}
		}
		s.recordRepair(ev)
	}
}

// checkReplica 检查一个副本位置的健康状态（纯检查，不修复）。
func (s *Store) checkReplica(rec CommitRecord, loc ReplicaLoc) ReplicaHealth {
	h := ReplicaHealth{
		ObjectID: rec.ObjectID, Block: rec.Block, Shard: loc.Shard,
		Replica: loc.Replica, Backend: loc.Backend,
		Path: VersionedShardPath(rec.ObjectID, rec.Block, loc.Shard, loc.Replica, rec.Version, rec.Writer),
	}
	back := s.backends[loc.Backend]
	if back == nil || !back.Available() {
		h.Status = ReplicaUnavailable
		h.Detail = "backend unavailable"
		return h
	}
	data, err := back.Read(h.Path)
	if err != nil {
		h.Status = ReplicaMissing
		h.Detail = fmt.Sprintf("read: %v", err)
		return h
	}
	if crc32.ChecksumIEEE(data) != rec.ShardCRC[loc.Shard] {
		h.Status = ReplicaCorrupt
		h.Detail = "shard crc mismatch"
		return h
	}
	h.Status = ReplicaOK
	h.Detail = "crc verified"
	return h
}

// Scrub 对当前所有存活对象做健康巡检，并自动修复可修复的副本。
//
// 持有 opMu 读锁：与读写并发，与 GC/Restore 互斥。修复只写回
// 提交记录中的既定位置；每个分片只要还有一个完好副本就能修好其余，
// 一个都不剩时计为降级（不静默）。
func (s *Store) Scrub() (*ScrubReport, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	s.mu.Lock()
	var recs []CommitRecord
	objects := map[string]bool{}
	for obj, blocks := range s.committed {
		objects[obj] = true
		for _, rs := range blocks {
			recs = append(recs, rs...)
		}
	}
	s.mu.Unlock()
	sort.Slice(recs, func(i, j int) bool { return recs[i].Key() < recs[j].Key() })

	rep := &ScrubReport{CheckedObjects: len(objects)}
	for _, rec := range recs {
		locs := append([]ReplicaLoc(nil), rec.Locs...)
		sort.Slice(locs, func(i, j int) bool {
			if locs[i].Shard != locs[j].Shard {
				return locs[i].Shard < locs[j].Shard
			}
			return locs[i].Replica < locs[j].Replica
		})
		shardHealthy := map[int]bool{}
		var findings []ReplicaHealth
		for _, loc := range locs {
			h := s.checkReplica(rec, loc)
			rep.CheckedReplicas++
			if h.Status == ReplicaOK {
				rep.OK++
				shardHealthy[loc.Shard] = true
			}
			findings = append(findings, h)
		}
		// 第二遍：修复非完好副本（源 = 该分片任一完好副本）。
		for i := range findings {
			h := &findings[i]
			if h.Status == ReplicaOK || h.Status == ReplicaUnavailable {
				continue // 不可用后端上的副本留待后端恢复后处理
			}
			src, err := s.readShardCopy(rec, h.Shard)
			if err != nil {
				rep.RepairFailed++
				h.Detail += "; repair failed: no intact source replica"
				s.recordRepair(RepairEvent{
					ObjectID: rec.ObjectID, Block: rec.Block, Shard: h.Shard,
					Replica: h.Replica, Backend: h.Backend, Path: h.Path,
					Trigger: "scrub", Detail: "no intact source replica",
				})
				continue
			}
			back := s.backends[h.Backend]
			werr := back.Write(h.Path, src)
			if werr == nil {
				// 回读校验，确保修复结果真实落盘且完好。
				got, rerr := back.Read(h.Path)
				if rerr != nil || crc32.ChecksumIEEE(got) != rec.ShardCRC[h.Shard] {
					werr = fmt.Errorf("verify after repair failed: %v", rerr)
				}
			}
			if werr != nil {
				rep.RepairFailed++
				h.Detail += "; repair failed: " + werr.Error()
				s.recordRepair(RepairEvent{
					ObjectID: rec.ObjectID, Block: rec.Block, Shard: h.Shard,
					Replica: h.Replica, Backend: h.Backend, Path: h.Path,
					Trigger: "scrub", Detail: "repair failed: " + werr.Error(),
				})
				continue
			}
			h.Repaired = true
			h.Detail += "; repaired in place from intact replica"
			rep.Repaired++
			shardHealthy[h.Shard] = true
			s.recordRepair(RepairEvent{
				ObjectID: rec.ObjectID, Block: rec.Block, Shard: h.Shard,
				Replica: h.Replica, Backend: h.Backend, Path: h.Path,
				OK: true, Trigger: "scrub", Detail: "repaired in place",
			})
		}
		rep.Findings = append(rep.Findings, findings...)
		for si := 0; si < s.cfg.Shards; si++ {
			if !shardHealthy[si] {
				rep.Degraded++
			}
		}
	}
	rep.Reason = fmt.Sprintf(
		"scrubbed %d objects: %d replicas ok, %d repaired in place, %d repair failures, %d shards still degraded; "+
			"repairs only rewrite committed locations, layout unchanged",
		rep.CheckedObjects, rep.OK, rep.Repaired, rep.RepairFailed, rep.Degraded)
	return rep, nil
}
