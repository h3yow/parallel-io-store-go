package store

// 本文件实现在线扩容与确定性重平衡。
//
// 语义：
//   - AddBackend 只把新后端加入健康环，不迁移任何已有数据（向后兼容：
//     旧数据、旧元数据、恢复流程完全不变）；
//   - Rebalance 按当前健康环 + 权重配置重算每个存活块的目标布局 ——
//     目标布局与新写入使用同一条确定性规则，只取决于
//     (对象, 块, 分片, 副本数, 后端配置)，与各后端当前剩余空间无关；
//   - 迁移逐个副本"先复制到目标位置并回读校验，再原子切换提交记录中的
//     位置"，切换前旧副本继续服务读取，因此整个过程中读取不会失败；
//   - 容量判定与新写入一致：目标位置预算内放不下时**暂缓**该块的迁移
//     （旧副本全部保留、照常可读，报告中记为 deferred），绝不硬塞导致
//     任何后端超出预算；等条件允许（预算调高/空间释放）后再次
//     Rebalance 即可继续；
//   - 迁移前在同一把锁内对目标后端做容量预留（与写入路径共用同一本账），
//     因此重平衡与并发写入同时发生时，任何后端已用+预留都不会超过预算；
//   - 过程可中断、可重试、可重复触发：已切换的块重算后幂等跳过，
//     中断留下的半成品复制文件不在存活集合内，由 GC 回收；
//     最终收敛到同一个布局，不产生多余副本、不丢块；
//   - 被取代的旧位置副本成为垃圾，由 GC 回收，回收后占用等于存活集合。

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
)

// RebalanceReport 是一次重平衡的可解释结果。
type RebalanceReport struct {
	PlannedMoves     int    // 计划迁移的副本数（目标位置与当前记录不同）
	CopiedReplicas   int    // 本轮实际复制并校验通过的副本数
	UpdatedBlocks    int    // 本轮切换了新布局的块数
	AlreadyConverged int    // 已在目标布局、无需任何动作的块数
	DeferredBlocks   int    // 因目标位置容量不足而暂缓迁移的块数（旧副本保留）
	DeferredMoves    int    // 被暂缓的副本迁移数
	Reason           string // 判定依据文字说明
}

// AddBackend 在运行中加入一个新后端（只影响后续写入的放置，不迁移数据）。
// 重复 ID 报错；只加后端不做 Rebalance 时，已有布局与读取路径完全不变。
func (s *Store) AddBackend(b *DiskBackend) error {
	if b == nil {
		return errors.New("store: nil backend")
	}
	s.mu.Lock()
	if _, dup := s.backends[b.id]; dup {
		s.mu.Unlock()
		return fmt.Errorf("store: duplicate backend id %q", b.id)
	}
	s.backends[b.id] = b
	s.mu.Unlock()
	if err := s.planner.AddBackend(b); err != nil {
		s.mu.Lock()
		delete(s.backends, b.id)
		s.mu.Unlock()
		return err
	}
	return nil
}

// locKey 标识一个 (分片, 副本) 位置。
type locKey struct{ Shard, Replica int }

// readShardCopy 从提交记录中该分片的任一完好副本读出字节并校验 CRC。
// 供重平衡/自愈复制使用；所有副本都不可读时返回错误（不静默）。
func (s *Store) readShardCopy(rec CommitRecord, shard int) ([]byte, error) {
	var lastErr error
	for _, loc := range rec.Locs {
		if loc.Shard != shard {
			continue
		}
		back := s.backends[loc.Backend]
		p := VersionedShardPath(rec.ObjectID, rec.Block, shard, loc.Replica, rec.Version, rec.Writer)
		if back == nil || !back.Available() {
			lastErr = fmt.Errorf("backend=%s unavailable", loc.Backend)
			continue
		}
		data, err := back.Read(p)
		if err != nil {
			lastErr = err
			continue
		}
		if crc32.ChecksumIEEE(data) != rec.ShardCRC[shard] {
			lastErr = fmt.Errorf("backend=%s crc mismatch", loc.Backend)
			continue
		}
		return data, nil
	}
	return nil, fmt.Errorf("%w: object=%s block=%d shard=%d no intact replica to copy from: %v",
		ErrBlockDegraded, rec.ObjectID, rec.Block, shard, lastErr)
}

// Rebalance 对当前全部存活块执行一次确定性重平衡。
//
// 持有 opMu 读锁：与普通读写并发，但排斥 GC/Restore —— 保证复制期间
// 旧副本不会被回收、提交视图不会被整体替换。布局切换以块为原子单位
// （WAL Relocate），读取在任何时刻都落在某个完整生效的布局上。
func (s *Store) Rebalance() (*RebalanceReport, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	ring := s.planner.HealthyBackends()
	if err := s.planner.CanPlace(len(ring)); err != nil {
		return nil, &PlacementError{
			ObjectID: "", Block: -1, Shard: -1,
			Want: s.cfg.Replicas, Healthy: len(ring), Cause: err,
			Decision: "rebalance preflight: healthy backend count < R; target layout uncomputable, nothing moved",
		}
	}
	// 快照当前生效记录（按 Key 排序，处理顺序确定）。
	s.mu.Lock()
	var recs []CommitRecord
	for _, blocks := range s.committed {
		for _, rs := range blocks {
			recs = append(recs, rs...)
		}
	}
	s.mu.Unlock()
	sort.Slice(recs, func(i, j int) bool { return recs[i].Key() < recs[j].Key() })

	rep := &RebalanceReport{}
	for _, rec := range recs {
		need := shardSizes(rec.Size, s.cfg.Shards)
		cur := map[locKey]string{}
		for _, loc := range rec.Locs {
			cur[locKey{loc.Shard, loc.Replica}] = loc.Backend
		}
		// 目标布局：与新写入完全相同的确定性规则，不看任何容量/占用信息，
		// 因此无论重平衡执行多少次、与多少并发写入交错，目标都一致。
		layout, err := s.planner.BlockLayout(rec.ObjectID, rec.Block, ring)
		if err != nil {
			return rep, fmt.Errorf("rebalance: object=%s block=%d: %w", rec.ObjectID, rec.Block, err)
		}
		var newLocs []ReplicaLoc
		converged := true
		for si := range layout.Shards {
			for _, pl := range layout.Shards[si] {
				newLocs = append(newLocs, ReplicaLoc{Shard: si, Replica: pl.Replica, Backend: pl.Backend})
				if cur[locKey{si, pl.Replica}] != pl.Backend {
					converged = false
				}
			}
		}
		if converged && len(cur) == len(newLocs) {
			rep.AlreadyConverged++
			continue
		}
		// 只迁移位置发生变化的副本；位置不变的副本不消耗任何新空间。
		var moves []ReplicaLoc
		for _, nl := range newLocs {
			if cur[locKey{nl.Shard, nl.Replica}] == nl.Backend {
				continue
			}
			moves = append(moves, nl)
		}
		rep.PlannedMoves += len(moves)
		// 容量判定（与新写入同一本账、同一把锁）：目标后端预算内剩余
		// 必须放得下待迁入的分片；放得下则立即预留，保证并发写入不会
		// 把这份容量重复花掉；放不下则暂缓整块迁移 —— 旧副本全部保留、
		// 照常可读，绝不为了完成重平衡把任何后端写爆。
		s.mu.Lock()
		avail := s.availableLocked()
		fitsAll := true
		consumed := map[string]int64{}
		for _, mv := range moves {
			bytes := need[mv.Shard]
			rem, ok := avail[mv.Backend]
			if !ok || rem < 0 {
				continue // 目标后端未配置预算 = 不限
			}
			if rem-consumed[mv.Backend] < bytes {
				fitsAll = false
				break
			}
			consumed[mv.Backend] += bytes
		}
		if !fitsAll {
			s.mu.Unlock()
			rep.DeferredBlocks++
			rep.DeferredMoves += len(moves)
			continue
		}
		delta := Layout{Shards: make([][]Placement, s.cfg.Shards)}
		for _, mv := range moves {
			delta.Shards[mv.Shard] = append(delta.Shards[mv.Shard], Placement{
				Block: rec.Block, Shard: mv.Shard, Replica: mv.Replica, Backend: mv.Backend,
			})
		}
		s.reserveLocked(delta, need)
		s.mu.Unlock()
		release := func() {
			s.mu.Lock()
			s.releaseReservedLocked(delta, need)
			s.mu.Unlock()
		}
		// 逐副本迁移：复制 -> 回读校验 -> 全部就位后原子切换记录。
		copied := 0
		for _, nl := range moves {
			data, err := s.readShardCopy(rec, nl.Shard)
			if err != nil {
				release()
				return rep, fmt.Errorf("rebalance: copy source: %w", err)
			}
			back := s.backends[nl.Backend]
			if back == nil || !back.Available() {
				release()
				return rep, fmt.Errorf("rebalance: %w: target backend=%s", ErrBackendUnavailable, nl.Backend)
			}
			path := VersionedShardPath(rec.ObjectID, rec.Block, nl.Shard, nl.Replica, rec.Version, rec.Writer)
			if err := back.Write(path, data); err != nil {
				release()
				return rep, fmt.Errorf("rebalance: write replica backend=%s path=%s: %w", nl.Backend, path, err)
			}
			// 回读校验：新副本未完整落盘且 CRC 正确之前，绝不切换记录。
			got, err := back.Read(path)
			if err != nil {
				release()
				return rep, fmt.Errorf("rebalance: verify read backend=%s path=%s: %w", nl.Backend, path, err)
			}
			if crc32.ChecksumIEEE(got) != rec.ShardCRC[nl.Shard] {
				release()
				return rep, fmt.Errorf("rebalance: verify crc backend=%s path=%s: %w", nl.Backend, path, ErrCheckpointCorrupt)
			}
			copied++
		}
		newRec := rec
		newRec.Locs = newLocs
		if err := s.wal.Relocate(newRec); err != nil {
			release()
			return rep, fmt.Errorf("rebalance: switch layout: %w", err)
		}
		s.mu.Lock()
		blocks := s.committed[rec.ObjectID]
		for i, ex := range blocks[rec.Block] {
			if ex.Key() == rec.Key() {
				blocks[rec.Block][i] = newRec
				break
			}
		}
		// 提交记录已切换：占用改由已提交记账接管，释放在途预留。
		s.releaseReservedLocked(delta, need)
		s.mu.Unlock()
		rep.CopiedReplicas += copied
		rep.UpdatedBlocks++
	}
	rep.Reason = fmt.Sprintf(
		"target layout recomputed with the same deterministic rule as new writes (depends only on object/block/shard/R/backend config, never on current free space); "+
			"%d blocks already converged, %d blocks switched after copy+verify (%d replicas copied), "+
			"%d blocks deferred (%d replicas) because target budgets cannot fit yet — old replicas kept and readable, retry when capacity allows; "+
			"old locations remain readable until switch and become GC garbage afterwards",
		rep.AlreadyConverged, rep.UpdatedBlocks, rep.CopiedReplicas, rep.DeferredBlocks, rep.DeferredMoves)
	return rep, nil
}
