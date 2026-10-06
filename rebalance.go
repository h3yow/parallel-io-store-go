package store

// 本文件实现在线扩容与确定性重平衡。
//
// 语义：
//   - AddBackend 只把新后端加入健康环，不迁移任何已有数据（向后兼容：
//     旧数据、旧元数据、恢复流程完全不变）；
//   - Rebalance 按当前健康环 + 权重配置重算每个存活块的目标布局
//     （位置是确定性的，与各后端当前剩余空间无关），逐个副本
//     "先复制到目标位置并回读校验，再原子切换提交记录中的位置"，
//     切换前旧副本继续服务读取，因此整个过程中读取不会失败；
//   - 搬迁副本与新写入遵循同一条容量规则：切换前先做容量增量判定并
//     预留（目标位置预算内放得下才搬）；放不下则**暂缓该块**——旧副本
//     继续保留、正常可读，等条件允许（预算调高 / GC 回收垃圾）后再搬，
//     绝不为了把重平衡跑完而把某个后端写爆；
//   - 过程可中断、可重试、可重复触发：已切换的块重算后幂等跳过，
//     被暂缓的块每轮重试；中断留下的半成品复制文件不在存活集合内，
//     由 GC 回收；最终收敛到同一个布局，不产生多余副本、不丢块；
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
	DeferredBlocks   int    // 因目标位置预算不足而暂缓的块数（旧副本保留，下轮重试）
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
// 持有 rebalMu：同一时刻至多一趟重平衡（容量增量判定会预支"即将释放
// 的旧位置"，串行化保证该不变量：已提交占用 <= 预算 全程成立）。
// 持有 opMu 读锁：与普通读写并发，但排斥 GC/Restore —— 保证复制期间
// 旧副本不会被回收、提交视图不会被整体替换。布局切换以块为原子单位
// （WAL Relocate），读取在任何时刻都落在某个完整生效的布局上。
func (s *Store) Rebalance() (*RebalanceReport, error) {
	s.rebalMu.Lock()
	defer s.rebalMu.Unlock()
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
		// 目标布局：只由 (对象, 块, 分片, 副本数, 当前健康环及其权重) 决定，
		// 与各后端当前剩余空间无关 —— 无论重算多少次、重试多少轮，
		// 目标位置都相同，这是收敛与幂等的基础。
		layout, err := s.planner.BlockLayout(rec.ObjectID, rec.Block, ring)
		if err != nil {
			return rep, fmt.Errorf("rebalance: object=%s block=%d: %w", rec.ObjectID, rec.Block, err)
		}
		var newLocs []ReplicaLoc
		cur := map[locKey]string{}
		for _, loc := range rec.Locs {
			cur[locKey{loc.Shard, loc.Replica}] = loc.Backend
		}
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
		// 容量增量判定（与新写入同一条规则）：切换后每个受影响后端的
		// 已用 - 释放 + 新增 + 他人预留 不得超预算。判定与预留在同一把
		// 锁内完成，因此与并发写入叠加也不会把任何后端写爆。
		adds := map[string]int64{}
		removes := map[string]int64{}
		stay := map[locKey]bool{}
		for _, nl := range newLocs {
			k := locKey{nl.Shard, nl.Replica}
			if cur[k] == nl.Backend {
				stay[k] = true
				continue
			}
			adds[nl.Backend] += shardSizeOf(rec.Size, s.cfg.Shards, nl.Shard)
		}
		for k, b := range cur {
			if !stay[k] {
				removes[b] += shardSizeOf(rec.Size, s.cfg.Shards, k.Shard)
			}
		}
		s.mu.Lock()
		ok := s.tryReserveDeltaLocked(adds, removes)
		s.mu.Unlock()
		if !ok {
			// 目标位置预算内放不下：暂缓该块。旧副本继续保留并服务读取，
			// 不产生任何新字节；下一轮（预算调高 / GC 之后）再试。
			rep.DeferredBlocks++
			continue
		}
		// 逐副本迁移：复制 -> 回读校验 -> 全部就位后原子切换记录。
		for _, nl := range newLocs {
			if cur[locKey{nl.Shard, nl.Replica}] == nl.Backend {
				continue // 该副本位置不变
			}
			rep.PlannedMoves++
			data, err := s.readShardCopy(rec, nl.Shard)
			if err != nil {
				s.mu.Lock()
				s.releaseDeltaLocked(adds)
				s.mu.Unlock()
				return rep, fmt.Errorf("rebalance: copy source: %w", err)
			}
			back := s.backends[nl.Backend]
			if back == nil || !back.Available() {
				s.mu.Lock()
				s.releaseDeltaLocked(adds)
				s.mu.Unlock()
				return rep, fmt.Errorf("rebalance: %w: target backend=%s", ErrBackendUnavailable, nl.Backend)
			}
			path := VersionedShardPath(rec.ObjectID, rec.Block, nl.Shard, nl.Replica, rec.Version, rec.Writer)
			if err := back.Write(path, data); err != nil {
				s.mu.Lock()
				s.releaseDeltaLocked(adds)
				s.mu.Unlock()
				return rep, fmt.Errorf("rebalance: write replica backend=%s path=%s: %w", nl.Backend, path, err)
			}
			// 回读校验：新副本未完整落盘且 CRC 正确之前，绝不切换记录。
			got, err := back.Read(path)
			if err != nil {
				s.mu.Lock()
				s.releaseDeltaLocked(adds)
				s.mu.Unlock()
				return rep, fmt.Errorf("rebalance: verify read backend=%s path=%s: %w", nl.Backend, path, err)
			}
			if crc32.ChecksumIEEE(got) != rec.ShardCRC[nl.Shard] {
				s.mu.Lock()
				s.releaseDeltaLocked(adds)
				s.mu.Unlock()
				return rep, fmt.Errorf("rebalance: verify crc backend=%s path=%s: %w", nl.Backend, path, ErrCheckpointCorrupt)
			}
			rep.CopiedReplicas++
		}
		newRec := rec
		newRec.Locs = newLocs
		if err := s.wal.Relocate(newRec); err != nil {
			s.mu.Lock()
			s.releaseDeltaLocked(adds)
			s.mu.Unlock()
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
		// 提交记录已切换：占用由已提交记账接管（新位置计入、旧位置释放），
		// 预留相应释放。
		s.releaseDeltaLocked(adds)
		s.mu.Unlock()
		rep.UpdatedBlocks++
	}
	rep.Reason = fmt.Sprintf(
		"target layout recomputed deterministically on current healthy ring (positions depend only on object/block/shard/R/backend set+weights); "+
			"%d blocks already converged, %d blocks switched after copy+verify (%d replicas copied), "+
			"%d blocks deferred because target budgets cannot fit them yet (old replicas kept, retried next round); "+
			"old locations remain readable until switch and become GC garbage afterwards",
		rep.AlreadyConverged, rep.UpdatedBlocks, rep.CopiedReplicas, rep.DeferredBlocks)
	return rep, nil
}
