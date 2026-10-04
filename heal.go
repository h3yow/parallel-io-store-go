package store

// 本文件实现读取自愈与巡检：
//   - 读取自愈：readBlock 发现某个副本缺失/损坏，但同分片仍有完好
//     副本时，照常返回正确内容，并顺手把完好字节补写回"提交记录中
//     固定的那个位置"（同一后端、同一版本化路径）。补写失败只记录
//     事件，绝不影响本次读取结果；补写绝不改变已生效的副本布局；
//   - 巡检（Scrub）：对当前生效提交视图的所有副本做一遍健康检查，
//     报告每个副本 好/坏/缺失/后端不可用 及位置，自动修复能修好的
//     （同源同位置补写），返回可解释统计：检查多少、修了多少、
//     还剩多少分片处于降级（无任何完好副本、无法修复）。

import (
	"fmt"
	"hash/crc32"
	"sort"
)

// HealEvent 记录一次读取路径上的自愈尝试（成功或失败都记录）。
type HealEvent struct {
	ObjectID string
	Block    int
	Shard    int
	Replica  int
	Backend  string
	Path     string
	Trigger  string // 触发原因（读失败/校验和不符）
	Repaired bool
	Err      string // 补写失败原因（成功为空）
}

// HealEvents 返回迄今为止记录的自愈事件（按发生顺序）。
func (s *Store) HealEvents() []HealEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]HealEvent(nil), s.healEvents...)
}

// recordHeal 记录一次自愈事件（线程安全）。
func (s *Store) recordHeal(ev HealEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.healEvents = append(s.healEvents, ev)
}

// repairReplica 把完好分片字节补写回提交记录指定的固定位置。
// 只写回该位置，不改变任何布局；失败由调用方记录，不向外传播。
func (s *Store) repairReplica(rec CommitRecord, shard, replica int, backendID string, data []byte) error {
	back := s.backends[backendID]
	if back == nil {
		return fmt.Errorf("backend=%s not configured", backendID)
	}
	path := VersionedShardPath(rec.ObjectID, rec.Block, shard, replica, rec.Version, rec.Writer)
	if err := back.Write(path, data); err != nil {
		return err
	}
	// 复读校验：确认补写后的字节与提交记录中的分片 CRC 一致。
	back2, err := back.Read(path)
	if err != nil {
		return fmt.Errorf("readback: %w", err)
	}
	if crc32.ChecksumIEEE(back2) != rec.ShardCRC[shard] {
		return fmt.Errorf("readback crc mismatch")
	}
	return nil
}

// 副本健康状态。
const (
	ReplicaOK          = "ok"
	ReplicaCorrupt     = "corrupt"
	ReplicaMissing     = "missing"
	ReplicaUnavailable = "backend-unavailable"
)

// ReplicaHealth 是巡检对单个副本的判定。
type ReplicaHealth struct {
	ObjectID  string
	Block     int
	Shard     int
	Replica   int
	Backend   string
	Path      string
	Status    string // ReplicaOK / ReplicaCorrupt / ReplicaMissing / ReplicaUnavailable
	Repaired  bool   // 本轮巡检是否将其修复
	RepairErr string // 修复失败原因
}

// ScrubReport 是一次巡检的可解释统计。
type ScrubReport struct {
	RecordsChecked  int
	ReplicasChecked int
	OK              int
	Corrupt         int
	Missing         int
	Unavailable     int
	Repaired        int
	RepairFailed    int
	DegradedShards  int // 已无任何完好副本、无法修复的分片数
	Replicas        []ReplicaHealth
	Reason          string
}

// Scrub 对当前生效提交视图的全部副本做健康检查并自动修复。
// 修复只写回提交记录中固定的位置（同后端同路径），布局不变；
// 持有 opMu 读锁，与并发读写兼容，与 GC/恢复互斥。
func (s *Store) Scrub() (*ScrubReport, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	rep := &ScrubReport{}

	s.mu.Lock()
	var recs []CommitRecord
	for _, blocks := range s.committed {
		for _, rs := range blocks {
			recs = append(recs, rs...)
		}
	}
	s.mu.Unlock()
	sort.Slice(recs, func(i, j int) bool { return recs[i].Key() < recs[j].Key() })

	for _, rec := range recs {
		rep.RecordsChecked++
		// 按分片分组该记录的副本位置。
		locsByShard := map[int][]ReplicaLoc{}
		for _, loc := range rec.Locs {
			locsByShard[loc.Shard] = append(locsByShard[loc.Shard], loc)
		}
		shards := make([]int, 0, len(locsByShard))
		for si := range locsByShard {
			shards = append(shards, si)
		}
		sort.Ints(shards)
		for _, si := range shards {
			locs := append([]ReplicaLoc(nil), locsByShard[si]...)
			sort.Slice(locs, func(i, j int) bool { return locs[i].Replica < locs[j].Replica })
			start := len(rep.Replicas) // 本分片健康条目在报告中的起始下标
			var intact []byte
			for _, loc := range locs {
				h := ReplicaHealth{
					ObjectID: rec.ObjectID, Block: rec.Block, Shard: si, Replica: loc.Replica,
					Backend: loc.Backend,
					Path:    VersionedShardPath(rec.ObjectID, rec.Block, si, loc.Replica, rec.Version, rec.Writer),
				}
				rep.ReplicasChecked++
				back := s.backends[loc.Backend]
				var data []byte
				switch {
				case back == nil || !back.Available():
					h.Status = ReplicaUnavailable
					rep.Unavailable++
				default:
					d, err := back.Read(h.Path)
					switch {
					case err != nil:
						h.Status = ReplicaMissing
						rep.Missing++
					case crc32.ChecksumIEEE(d) != rec.ShardCRC[si]:
						h.Status = ReplicaCorrupt
						rep.Corrupt++
					default:
						h.Status = ReplicaOK
						rep.OK++
						data = d
					}
				}
				if h.Status == ReplicaOK {
					if intact == nil {
						intact = data
					}
				}
				rep.Replicas = append(rep.Replicas, h)
			}
			// 第二遍：用完好字节修复坏/缺失副本（位置固定，布局不变）。
			if intact == nil {
				rep.DegradedShards++ // 本分片已无任何完好副本，无法修复
				continue
			}
			for i := start; i < len(rep.Replicas); i++ {
				h := &rep.Replicas[i]
				if h.Status == ReplicaOK {
					continue
				}
				if err := s.repairReplica(rec, si, h.Replica, h.Backend, intact); err != nil {
					h.RepairErr = err.Error()
					rep.RepairFailed++
					continue
				}
				h.Repaired = true
				rep.Repaired++
			}
		}
	}
	rep.Reason = fmt.Sprintf(
		"scrubbed live committed view: records=%d replicas=%d ok=%d corrupt=%d missing=%d unavailable=%d; "+
			"repaired=%d repairFailed=%d degradedShards=%d (repair writes good bytes back to the exact committed location only)",
		rep.RecordsChecked, rep.ReplicasChecked, rep.OK, rep.Corrupt, rep.Missing, rep.Unavailable,
		rep.Repaired, rep.RepairFailed, rep.DegradedShards)
	return rep, nil
}
