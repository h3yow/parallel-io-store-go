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
	all      []*DiskBackend // 构造时给定，按 ID 排序
	backends []*DiskBackend // all 的别名，语义见 HealthyBackends
	s        int            // 每块分片数
	r        int            // 每分片副本数
}

// NewPlanner 创建放置规划器。
func NewPlanner(backends []*DiskBackend, shards, replicas int) *Planner {
	sorted := append([]*DiskBackend(nil), backends...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].id < sorted[j].id })
	return &Planner{all: sorted, backends: sorted, s: shards, r: replicas}
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

// BlockLayout 基于给定的有序后端环计算指定块的布局（纯函数）。
// 调用方负责保证 ring 只包含健康后端；副本数不足时返回 PlacementError。
//
// 权重规则：先把环按"ID 升序、每个后端重复 weight 次"展开成加权环，
// 再按 (h(obj,block,shard)+r) 在加权环上取槽位；若槽位对应的后端在本
// 分片内已被选中，则确定性地顺移到下一个未使用的后端，保证 R 个副本
// 落在互不相同的后端上。所有权重为 1 时展开环与原环完全一致，
// 布局与旧版本逐位相同（向后兼容）。权重只改变命中槽位的概率分布，
// 整个过程无任何随机性：同一输入永远得到同一布局。
func (p *Planner) BlockLayout(objectID string, block int, ring []*DiskBackend) (Layout, error) {
	if len(ring) < p.r {
		return Layout{}, &PlacementError{
			ObjectID: objectID, Block: block,
			Shard: -1, Want: p.r, Healthy: len(ring),
			Cause:    ErrReplicaShortage,
			Decision: fmt.Sprintf("rule: shard s replica r -> weightedRing[(h(obj,b,s)+r) mod W] with deterministic skip to distinct backend; N=%d healthy backends < R=%d", len(ring), p.r),
		}
	}
	ids := make([]string, len(ring))
	for i, b := range ring {
		ids[i] = b.id
	}
	// 展开加权环：按 ID 升序，每个后端重复 weight 次（确定性）。
	var expanded []*DiskBackend
	for _, b := range ring {
		for w := 0; w < b.Weight(); w++ {
			expanded = append(expanded, b)
		}
	}
	lay := Layout{
		ObjectID: objectID, Block: block,
		Shards:   make([][]Placement, p.s),
		Backends: ids, S: p.s, R: p.r,
	}
	for sIdx := 0; sIdx < p.s; sIdx++ {
		start := int(shardStartHash(objectID, block, sIdx) % uint32(len(expanded)))
		lay.Shards[sIdx] = make([]Placement, p.r)
		used := map[string]bool{}
		for rIdx := 0; rIdx < p.r; rIdx++ {
			// 从 (start+rIdx) 起顺移到本分片尚未使用的后端（确定性去重）。
			var pick *DiskBackend
			for off := rIdx; off < len(expanded); off++ {
				cand := expanded[(start+off)%len(expanded)]
				if !used[cand.id] {
					pick = cand
					break
				}
			}
			if pick == nil {
				return Layout{}, &PlacementError{
					ObjectID: objectID, Block: block, Shard: sIdx,
					Want: p.r, Healthy: len(ring),
					Cause:    ErrReplicaShortage,
					Decision: "weighted ring exhausted without finding distinct backend",
				}
			}
			used[pick.id] = true
			lay.Shards[sIdx][rIdx] = Placement{
				Block:   block,
				Shard:   sIdx,
				Replica: rIdx,
				Backend: pick.id,
				Path:    ShardPath(objectID, block, sIdx, rIdx),
			}
		}
	}
	return lay, nil
}

// HealthyBackends 返回当前可用后端，顺序按 ID 排序（确定性）。
// 布局计算永远基于"当前健康环"，不可用后端不会被选中，
// 因此故障后端上不会静默放置新副本。
func (p *Planner) HealthyBackends() []*DiskBackend {
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
