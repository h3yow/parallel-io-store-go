package store

// 本文件实现后端的容量预算与相对权重，以及按后端的占用统计。
//
// 语义：
//   - 每个后端可配置容量预算（字节，0 表示不限）与相对权重（>=1）；
//   - 放置副本时同时受"预算内剩余空间"与"权重分布倾向"约束，
//     两者都是确定性的：同一组输入永远算出同一布局；
//   - 已提交数据占用的空间不会被后续写入挤占：写入在落盘任何字节前
//     先做容量预检并预留，预检失败整体失败；
//   - 占用统计（预算/已用/预留/剩余）从"当前生效提交集合"精确推导，
//     与磁盘上的存活数据严格一致。

import "sort"

// BackendUsage 是单个后端的容量占用视图。
type BackendUsage struct {
	Backend   string
	Budget    int64 // 容量预算（字节）；0 表示不限
	Weight    int   // 相对权重（>=1）
	Used      int64 // 当前生效提交集合在该后端上占用的字节
	Reserved  int64 // 在途写入已预留、尚未提交的字节
	Remaining int64 // 预算内剩余；预算不限时为 -1
}

// SetCapacity 设置后端的容量预算（字节，0=不限）与相对权重（<1 视为 1）。
func (b *DiskBackend) SetCapacity(budgetBytes int64, weight int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if budgetBytes < 0 {
		budgetBytes = 0
	}
	if weight < 1 {
		weight = 1
	}
	b.budget = budgetBytes
	b.weight = weight
}

// Capacity 返回后端配置的容量预算与相对权重。
func (b *DiskBackend) Capacity() (budgetBytes int64, weight int) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.budget, b.weight
}

// shardSizeOf 返回块内第 idx 个字节轮转分片的字节数（与 splitShards 一致）。
func shardSizeOf(size, shards, idx int) int64 {
	n := (size - idx + shards - 1) / shards
	if n < 0 {
		n = 0
	}
	return int64(n)
}

// shardSizes 返回一个 size 字节块各分片的字节数。
func shardSizes(size, shards int) []int64 {
	out := make([]int64, shards)
	for i := range out {
		out[i] = shardSizeOf(size, shards, i)
	}
	return out
}

// usedLocked 计算当前生效提交集合在各后端上占用的字节（调用方须持有 s.mu）。
// 这是容量记账的唯一事实来源：与 GC 的存活根集逐文件对应。
func (s *Store) usedLocked() map[string]int64 {
	used := map[string]int64{}
	for _, blocks := range s.committed {
		for _, recs := range blocks {
			for _, rec := range recs {
				for _, loc := range rec.Locs {
					used[loc.Backend] += shardSizeOf(rec.Size, s.cfg.Shards, loc.Shard)
				}
			}
		}
	}
	return used
}

// availableLocked 返回各后端"预算内剩余"（预算 0 = 不限，记为 -1）。
func (s *Store) availableLocked() map[string]int64 {
	used := s.usedLocked()
	avail := map[string]int64{}
	for id, b := range s.backends {
		budget, _ := b.Capacity()
		if budget <= 0 {
			avail[id] = -1
			continue
		}
		avail[id] = budget - used[id] - s.reserved[id]
	}
	return avail
}

// reserve 为一次块写入按布局预留容量（调用方须持有 s.mu）。
func (s *Store) reserveLocked(layout Layout, need []int64) {
	for si := range layout.Shards {
		for _, pl := range layout.Shards[si] {
			s.reserved[pl.Backend] += need[si]
		}
	}
}

// releaseReservedLocked 释放 reserveLocked 建立的预留（调用方须持有 s.mu）。
func (s *Store) releaseReservedLocked(layout Layout, need []int64) {
	for si := range layout.Shards {
		for _, pl := range layout.Shards[si] {
			s.reserved[pl.Backend] -= need[si]
			if s.reserved[pl.Backend] == 0 {
				delete(s.reserved, pl.Backend)
			}
		}
	}
}

// CapacityReport 汇总全部后端的预算/已用/预留/剩余（按后端 ID 排序，确定）。
// Used 与磁盘上存活分片文件的字节总和严格一致（GC 之后 SpaceUsage 可验证）。
func (s *Store) CapacityReport() []BackendUsage {
	s.mu.Lock()
	defer s.mu.Unlock()
	used := s.usedLocked()
	ids := make([]string, 0, len(s.backends))
	for id := range s.backends {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]BackendUsage, 0, len(ids))
	for _, id := range ids {
		budget, weight := s.backends[id].Capacity()
		u := BackendUsage{
			Backend:  id,
			Budget:   budget,
			Weight:   weight,
			Used:     used[id],
			Reserved: s.reserved[id],
		}
		if budget > 0 {
			u.Remaining = budget - u.Used - u.Reserved
		} else {
			u.Remaining = -1
		}
		out = append(out, u)
	}
	return out
}
