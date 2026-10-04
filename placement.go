package store

// 本文件实现确定性的分片布局规则：
//   - 逻辑对象按固定块大小切块；
//   - 每个块按字节轮转切成 S 个分片；
//   - 每个分片有 R 个副本，分布在互不相同的后端上；
//   - 位置由 (对象标识, 块号, 分片号, 副本号) 经 FNV-1a 哈希推导，
//     任何节点在相同后端集合下得到完全相同的布局，不依赖写入顺序。

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Placement 是一个分片副本的放置决策。
type Placement struct {
	Block   int
	Shard   int
	Replica int
	Backend string
	Path    string // 该副本在后端内的相对路径
}

// Layout 是一个块的完整布局。
type Layout struct {
	ObjectID string
	Block    int
	Shards   [][]Placement // [shardIndex][replicaIndex]
	Backends []string      // 计算布局时使用的有序后端 ID 列表（健康视图）
	S        int           // 分片数
	R        int           // 每分片副本数
}

// Planner 负责确定性地计算分片布局。
type Planner struct {
	mu  sync.RWMutex
	all []*DiskBackend // 构造时给定、按 ID 排序；AddBackend 插入后仍有序
	s   int            // 每块分片数
	r   int            // 每分片副本数
}

// NewPlanner 创建放置规划器。
func NewPlanner(backends []*DiskBackend, shards, replicas int) *Planner {
	sorted := append([]*DiskBackend(nil), backends...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].id < sorted[j].id })
	return &Planner{all: sorted, s: shards, r: replicas}
}

// AddBackend 把新后端插入规划器视图（保持 ID 有序；重复 ID 返回错误）。
// 只影响之后的放置计算，不触碰任何已提交记录。
func (p *Planner) AddBackend(b *DiskBackend) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, ex := range p.all {
		if ex.id == b.id {
			return fmt.Errorf("planner: duplicate backend id %q", b.id)
		}
	}
	p.all = append(p.all, b)
	sort.Slice(p.all, func(i, j int) bool { return p.all[i].id < p.all[j].id })
	return nil
}

// S 返回每块分片数。
func (p *Planner) S() int { return p.s }

// R 返回每分片副本数。
func (p *Planner) R() int { return p.r }

// shardStartHash 计算一个分片在后端环上的起点下标。
func shardStartHash(objectID string, block, shard int) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(objectID))
	_, _ = h.Write([]byte{0})
	var buf [8]byte
	putInt := func(v int) {
		for i := 0; i < 8; i++ {
			buf[i] = byte(uint64(v) >> (uint(i) * 8))
		}
		_, _ = h.Write(buf[:])
	}
	putInt(block)
	putInt(shard)
	return h.Sum32()
}

// weightedRing 把有序健康环按相对权重展开为"加权环"：
// 每个后端按其权重出现多次，采用平滑加权轮转（smooth weighted
// round-robin）交错，使同一后端的副本在环上均匀散开、避免聚集。
// 当所有权重都为 1 时，加权环与原环逐元素相同 —— 因此未配置权重的
// 旧部署布局逐位不变（向后兼容）；权重只改变分布倾向，不引入任何随机性。
func weightedRing(ring []*DiskBackend) []*DiskBackend {
	weights := make([]int, len(ring))
	current := make([]int, len(ring))
	total := 0
	for i, b := range ring {
		_, w := b.Capacity()
		if w < 1 {
			w = 1
		}
		weights[i] = w
		total += w
	}
	expanded := make([]*DiskBackend, 0, total)
	for len(expanded) < total {
		best := -1
		for i := range ring {
			current[i] += weights[i]
			if best == -1 || current[i] > current[best] {
				best = i
			}
		}
		current[best] -= total
		expanded = append(expanded, ring[best])
	}
	return expanded
}

// BlockLayout 基于给定的有序后端环计算指定块的布局（纯函数）。
// 等价于 BlockLayoutWithCapacity 传入"无容量约束"。
func (p *Planner) BlockLayout(objectID string, block int, ring []*DiskBackend) (Layout, error) {
	return p.BlockLayoutWithCapacity(objectID, block, ring, nil, nil)
}

// BlockLayoutWithCapacity 在确定性规则上叠加容量约束计算块布局：
//
//   - need[shard] 为该分片单个副本的字节数；nil 表示不关心容量；
//   - avail 为后端 ID -> 剩余可用字节（<0 表示不限）；nil 表示全部不限；
//   - 选择规则：分片 s 从加权环位置 h(obj,block,s) mod L 起顺序扫描一整圈，
//     依次挑出 R 个"互不相同且剩余容量足够"的后端；同一块内先被选中的
//     分片会消耗对应后端的剩余容量（按分片号顺序，确定性）；
//   - 全部权重为 1 且无容量约束时，结果与旧规则 ring[(h+r) mod N] 完全一致；
//   - 健康后端数 < R 时失败原因为 ErrReplicaShortage；后端数量足够但
//     预算内放不下时为 ErrCapacityExhausted —— 两者可区分。
func (p *Planner) BlockLayoutWithCapacity(objectID string, block int, ring []*DiskBackend, need []int64, avail map[string]int64) (Layout, error) {
	if len(ring) < p.r {
		return Layout{}, &PlacementError{
			ObjectID: objectID, Block: block,
			Shard: -1, Want: p.r, Healthy: len(ring),
			Cause:    ErrReplicaShortage,
			Decision: fmt.Sprintf("rule: shard s replica r -> first R distinct backends with enough budget on weighted ring from (h(obj,b,s)) ; N=%d healthy backends < R=%d", len(ring), p.r),
		}
	}
	expanded := weightedRing(ring)
	L := len(expanded)
	// 本块内已消耗的容量（在 avail 基础上递减，确定性）。
	consumed := map[string]int64{}
	fits := func(id string, bytes int64) bool {
		if avail == nil || need == nil {
			return true
		}
		rem, ok := avail[id]
		if !ok || rem < 0 {
			return true // 未配置预算 = 不限
		}
		return rem-consumed[id] >= bytes
	}
	ids := make([]string, len(ring))
	for i, b := range ring {
		ids[i] = b.id
	}
	lay := Layout{
		ObjectID: objectID, Block: block,
		Shards:   make([][]Placement, p.s),
		Backends: ids, S: p.s, R: p.r,
	}
	for sIdx := 0; sIdx < p.s; sIdx++ {
		var shardBytes int64
		if need != nil && sIdx < len(need) {
			shardBytes = need[sIdx]
		}
		start := int(shardStartHash(objectID, block, sIdx) % uint32(L))
		chosen := make([]Placement, 0, p.r)
		usedHere := map[string]bool{}
		for k := 0; k < L && len(chosen) < p.r; k++ {
			b := expanded[(start+k)%L]
			if usedHere[b.id] {
				continue // 同一分片的副本必须在不同后端
			}
			if !fits(b.id, shardBytes) {
				continue // 预算内放不下：确定性跳过
			}
			usedHere[b.id] = true
			chosen = append(chosen, Placement{
				Block:   block,
				Shard:   sIdx,
				Replica: len(chosen),
				Backend: b.id,
				Path:    ShardPath(objectID, block, sIdx, len(chosen)),
			})
		}
		if len(chosen) < p.r {
			return Layout{}, &PlacementError{
				ObjectID: objectID, Block: block,
				Shard: sIdx, Want: p.r, Healthy: len(ring),
				Cause: ErrCapacityExhausted,
				Decision: fmt.Sprintf("rule: distinct healthy backends (%d) >= R=%d, but budgets cannot fit shard %d (%d bytes/replica) for R replicas; block aborted before any byte written",
					len(ring), p.r, sIdx, shardBytes),
			}
		}
		for _, pl := range chosen {
			consumed[pl.Backend] += shardBytes
		}
		lay.Shards[sIdx] = chosen
	}
	return lay, nil
}

// HealthyBackends 返回当前可用后端，顺序按 ID 排序（确定性）。
// 布局计算永远基于"当前健康环"，不可用后端不会被选中，
// 因此故障后端上不会静默放置新副本。
func (p *Planner) HealthyBackends() []*DiskBackend {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*DiskBackend, 0, len(p.all))
	for _, b := range p.all {
		if b.Available() {
			out = append(out, b)
		}
	}
	return out
}

// CanPlace 判断给定健康后端数是否能满足副本放置。
func (p *Planner) CanPlace(healthyCount int) error {
	if healthyCount < p.r {
		return fmt.Errorf("%w: healthy=%d replicas=%d", ErrReplicaShortage, healthyCount, p.r)
	}
	return nil
}

// escapeObjectID 将对象 ID 转成文件系统安全的单个路径段。
func escapeObjectID(objectID string) string {
	var sb strings.Builder
	for _, c := range objectID {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_', c == '.':
			sb.WriteRune(c)
		default:
			sb.WriteString("%")
			sb.WriteString(strings.ToUpper(strconv.FormatInt(int64(c), 16)))
		}
	}
	if sb.Len() == 0 {
		return "_"
	}
	return sb.String()
}

// ShardPath 返回某分片副本在其后端内的相对存储路径（布局路径，不含版本）。
// 规则: shards/<objEscaped>/b<block>/s<shard>/r<replica>.data
func ShardPath(objectID string, block, shard, replica int) string {
	return fmt.Sprintf("shards/%s/b%d/s%d/r%d.data", escapeObjectID(objectID), block, shard, replica)
}

// VersionedShardPath 返回带版本目录的实际存储路径。
// 规则: shards/<obj>/b<blk>/v<version>-<writer>/s<shard>/r<replica>.data
//
// 版本隔离保证：新版本的写入绝不可能覆盖旧版本副本，因此
// "新版本只写了一半"时旧版本仍然完整可读、不会出现悬空副本覆盖。
func VersionedShardPath(objectID string, block, shard, replica int, version int64, writer string) string {
	return fmt.Sprintf("shards/%s/b%d/v%d-%s/s%d/r%d.data",
		escapeObjectID(objectID), block, version, escapeObjectID(writer), shard, replica)
}

// shardVersionDir 返回块的版本目录，供 GC 枚举。
func shardVersionDir(objectID string, block int) string {
	return fmt.Sprintf("shards/%s/b%d", escapeObjectID(objectID), block)
}
