package store

// 本文件实现在线扩容后的确定性重平衡：
//   - 目标布局 = 用"当前健康环（含新后端）"对每条已提交记录重算
//     BlockLayout，与写入路径完全同一套确定性规则，因此无论重平衡
//     执行多少次、被中断多少次，最终都收敛到同一个布局；
//   - 搬迁单位是一条提交记录：先把需要移动的分片副本从完好源副本
//     读出（CRC 校验）写入目标后端的同一版本化路径，再校验目标
//     字节，最后才向 WAL 追加 KindReloc 记录切换提交视图 ——
//     在新副本完整落盘并通过校验之前，旧位置始终是存活集，
//     读取任何时刻都不会失败；
//   - 每条记录的"拷贝 → 校验 → 追加重定位记录"序列都是幂等的：
//     中断后重试只会重做尚未生效的部分，不产生多余副本、不丢块；
//   - 重定位生效后，旧位置文件不再被任何提交记录引用，
//     由 GC 回收，回收后各后端占用与存活副本集合严格相等；
//   - 目标后端容量预算不足时，该记录整体暂缓（不写任何字节），
//     旧位置继续服务，报告与错误中给出可区分的
//     ErrCapacityExhausted 原因。

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
)

// RebalanceOptions 控制重平衡执行。
type RebalanceOptions struct {
	// RecordLimit > 0 时本轮最多处理前 N 条记录（按 Key 排序），
	// 用于分批推进或测试"中断后重试"。
	RecordLimit int
	// Hook 在每处理完一条记录后回调（done 为已处理数，total 为本轮总数）；
	// 返回非 nil 错误则中止本轮，已生效的重定位保持有效，可安全重试。
	Hook func(done, total int) error
}

// ReplicaMove 描述一个副本的搬迁动作（判定依据）。
type ReplicaMove struct {
	ObjectID string
	Block    int
	Shard    int
	Replica  int
	From     string
	To       string
	Path     string
	Bytes    int
}

// RebalanceReport 是一轮重平衡的可解释报告。
type RebalanceReport struct {
	RecordsExamined int
	RecordsMoved    int
	ReplicasCopied  int
	BytesCopied     int64
	Moves           []ReplicaMove
	Deferred        []string // 暂缓记录及原因（容量不足/源不可读等）
	Reason          string
}

// Rebalance 对全部已提交记录执行一次确定性重平衡。
// 持有 opMu 读锁：并发读写照常进行（读取永不失败），
// 但 GC / 检查点恢复 / AddBackend 在本轮期间被互斥，
// 保证"旧位置文件"在搬迁完成前不会被回收。
func (s *Store) Rebalance(opts *RebalanceOptions) (*RebalanceReport, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	rep := &RebalanceReport{}

	// 1) 快照当前已提交记录（按 Key 排序，处理顺序确定）。
	s.mu.Lock()
	var recs []CommitRecord
	for _, blocks := range s.committed {
		for _, rs := range blocks {
			recs = append(recs, rs...)
		}
	}
	s.mu.Unlock()
	sort.Slice(recs, func(i, j int) bool { return recs[i].Key() < recs[j].Key() })
	total := len(recs)
	if opts != nil && opts.RecordLimit > 0 && opts.RecordLimit < total {
		total = opts.RecordLimit
	}

	ring := s.planner.HealthyBackends()
	if err := s.planner.CanPlace(len(ring)); err != nil {
		rep.Reason = fmt.Sprintf("healthy backends=%d < R=%d; nothing moved", len(ring), s.cfg.Replicas)
		return rep, err
	}

	var deferred []error
	for i := 0; i < total; i++ {
		rec := recs[i]
		moved, derr := s.relocateRecord(rec, ring, rep)
		if derr != nil {
			deferred = append(deferred, derr)
			rep.Deferred = append(rep.Deferred, derr.Error())
		}
		if moved {
			rep.RecordsMoved++
		}
		rep.RecordsExamined++
		if opts != nil && opts.Hook != nil {
			if herr := opts.Hook(i+1, total); herr != nil {
				rep.Reason = fmt.Sprintf("aborted by hook after %d/%d records: %v", i+1, total, herr)
				return rep, herr
			}
		}
	}
	rep.Reason = fmt.Sprintf(
		"target layout = same deterministic rule as writes over current healthy ring (%s); "+
			"examined=%d moved=%d copied=%d replicas (%d bytes) deferred=%d",
		strings.Join(backendIDs(ring), ","), rep.RecordsExamined, rep.RecordsMoved,
		rep.ReplicasCopied, rep.BytesCopied, len(rep.Deferred))
	if len(deferred) > 0 {
		return rep, errors.Join(deferred...)
	}
	return rep, nil
}

func backendIDs(ring []*DiskBackend) []string {
	ids := make([]string, len(ring))
	for i, b := range ring {
		ids[i] = b.id
	}
	return ids
}

// relocateRecord 搬迁一条提交记录；返回是否发生位置变化。
// 任何失败都保证：不写半套位置、不改变提交视图，旧位置继续存活。
func (s *Store) relocateRecord(rec CommitRecord, ring []*DiskBackend, rep *RebalanceReport) (bool, error) {
	layout, err := s.planner.BlockLayout(rec.ObjectID, rec.Block, ring)
	if err != nil {
		return false, fmt.Errorf("reloc %s: %w", rec.Key(), err)
	}
	// 目标位置表：shard -> replica -> backend。
	target := map[int]map[int]string{}
	for si := range layout.Shards {
		target[si] = map[int]string{}
		for _, pl := range layout.Shards[si] {
			target[si][pl.Replica] = pl.Backend
		}
	}
	cur := map[int]map[int]string{}
	for _, loc := range rec.Locs {
		if cur[loc.Shard] == nil {
			cur[loc.Shard] = map[int]string{}
		}
		cur[loc.Shard][loc.Replica] = loc.Backend
	}
	// 计算需要移动的副本与容量需求。
	type mv struct {
		shard, replica int
		from, to       string
	}
	var moves []mv
	need := map[string]int64{}
	sizes := shardByteSizes(rec.Size, s.cfg.Shards)
	for si := range target {
		for ri, to := range target[si] {
			from := cur[si][ri]
			if from == to {
				continue
			}
			moves = append(moves, mv{si, ri, from, to})
			need[to] += int64(sizes[si])
		}
	}
	if len(moves) == 0 {
		return false, nil // 已在目标布局：幂等
	}
	sort.Slice(moves, func(i, j int) bool {
		if moves[i].shard != moves[j].shard {
			return moves[i].shard < moves[j].shard
		}
		return moves[i].replica < moves[j].replica
	})

	// 容量预检：目标后端放不下则整条记录暂缓，不写任何字节。
	s.mu.Lock()
	cerr := s.checkAndReserveLocked(rec.ObjectID, rec.Block, need)
	s.mu.Unlock()
	if cerr != nil {
		return false, fmt.Errorf("reloc %s deferred: %w", rec.Key(), cerr)
	}
	release := func() { s.unreserve(need) }

	// 逐副本拷贝：源必须 CRC 完好，目标写后复读校验。
	for _, m := range moves {
		data, src, rerr := s.readIntactShard(rec, m.shard)
		if rerr != nil {
			release()
			return false, fmt.Errorf("reloc %s shard=%d: no intact source replica: %w", rec.Key(), m.shard, rerr)
		}
		dst := s.backends[m.to]
		if dst == nil || !dst.Available() {
			release()
			return false, fmt.Errorf("reloc %s shard=%d: target backend=%s: %w",
				rec.Key(), m.shard, m.to, ErrBackendUnavailable)
		}
		path := VersionedShardPath(rec.ObjectID, rec.Block, m.shard, m.replica, rec.Version, rec.Writer)
		if werr := dst.Write(path, data); werr != nil {
			release()
			return false, fmt.Errorf("reloc %s shard=%d replica=%d -> %s: %w",
				rec.Key(), m.shard, m.replica, m.to, werr)
		}
		// 落盘后复读校验：只有校验通过才允许切换提交视图。
		back, rerr2 := dst.Read(path)
		if rerr2 != nil || crc32.ChecksumIEEE(back) != rec.ShardCRC[m.shard] {
			release()
			return false, fmt.Errorf("reloc %s shard=%d replica=%d -> %s: verify failed (readback err=%v)",
				rec.Key(), m.shard, m.replica, m.to, rerr2)
		}
		rep.Moves = append(rep.Moves, ReplicaMove{
			ObjectID: rec.ObjectID, Block: rec.Block, Shard: m.shard, Replica: m.replica,
			From: src, To: m.to, Path: path, Bytes: len(data),
		})
		rep.ReplicasCopied++
		rep.BytesCopied += int64(len(data))
	}

	// 全部新副本完整落盘并校验通过：切换提交视图（WAL 重定位记录）。
	newRec := rec
	newRec.Kind = KindReloc
	newRec.Locs = make([]ReplicaLoc, 0, len(rec.Locs))
	for si := range target {
		for ri, to := range target[si] {
			newRec.Locs = append(newRec.Locs, ReplicaLoc{Shard: si, Replica: ri, Backend: to})
		}
	}
	sort.Slice(newRec.Locs, func(i, j int) bool {
		if newRec.Locs[i].Shard != newRec.Locs[j].Shard {
			return newRec.Locs[i].Shard < newRec.Locs[j].Shard
		}
		return newRec.Locs[i].Replica < newRec.Locs[j].Replica
	})
	if err := s.applyRelocation(newRec); err != nil {
		release()
		return false, fmt.Errorf("reloc %s: commit reloc record: %w", rec.Key(), err)
	}
	release()
	return true, nil
}

// readIntactShard 从记录的现有副本中读出一个 CRC 完好的分片内容。
func (s *Store) readIntactShard(rec CommitRecord, shard int) ([]byte, string, error) {
	var locs []ReplicaLoc
	for _, loc := range rec.Locs {
		if loc.Shard == shard {
			locs = append(locs, loc)
		}
	}
	sort.Slice(locs, func(i, j int) bool { return locs[i].Replica < locs[j].Replica })
	var errs []string
	for _, loc := range locs {
		back := s.backends[loc.Backend]
		if back == nil || !back.Available() {
			errs = append(errs, fmt.Sprintf("backend=%s unavailable", loc.Backend))
			continue
		}
		p := VersionedShardPath(rec.ObjectID, rec.Block, shard, loc.Replica, rec.Version, rec.Writer)
		data, err := back.Read(p)
		if err != nil {
			errs = append(errs, fmt.Sprintf("backend=%s read: %v", loc.Backend, err))
			continue
		}
		if crc32.ChecksumIEEE(data) != rec.ShardCRC[shard] {
			errs = append(errs, fmt.Sprintf("backend=%s crc mismatch", loc.Backend))
			continue
		}
		return data, loc.Backend, nil
	}
	return nil, "", fmt.Errorf("%w: %s", ErrBlockDegraded, strings.Join(errs, "; "))
}

