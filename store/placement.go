package store

import (
	"fmt"
	"sort"
	"strings"
)

// Placement 描述一个 chunk 副本的确定性放置结果。
type Placement struct {
	LogicalBlock uint64
	ChunkIndex   int
	ReplicaIndex int
	Backend      string // 被选中的后端名
	BlockID      string // 该副本在后端上的块文件名（内容寻址）
	Checksum     string // chunk 数据完整 sha256
	Data         []byte // 该 chunk 的明文数据
}

// ChunkCount 返回给定长度数据按 chunkSize 切分后的 chunk 数。
func ChunkCount(dataLen, chunkSize int) int {
	if dataLen <= 0 || chunkSize <= 0 {
		return 0
	}
	return (dataLen + chunkSize - 1) / chunkSize
}

// PlacementPlan 汇总一次逻辑块写入的完整条带布局。
type PlacementPlan struct {
	LogicalBlock  uint64
	ChunkSize     int
	Length        int
	Backends      []string // 参与决策的全部后端（按名字排序的固定顺序）
	Chunks        [][]Placement
	RequestedReps int
}

// Explain 以可读形式打印分片布局与判定依据（供测试日志输出）。
func (p *PlacementPlan) Explain() string {
	if p == nil {
		return "<nil plan>"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "placement logical-block=%d length=%d chunk-size=%d backends=%v replicas=%d chunks=%d; ",
		p.LogicalBlock, p.Length, p.ChunkSize, p.Backends, p.RequestedReps, len(p.Chunks))
	fmt.Fprintf(&b, "rule: backend(chunk i, replica r) = sorted-backends[(logicalBlock+i+r) mod N]")
	for i, reps := range p.Chunks {
		fmt.Fprintf(&b, "\n  chunk[%d] len=%d checksum=%s:", i, len(reps[0].Data), reps[0].Checksum)
		for _, pl := range reps {
			fmt.Fprintf(&b, "\n    replica[%d] -> backend=%s block=%s",
				pl.ReplicaIndex, pl.Backend, pl.BlockID)
		}
	}
	return b.String()
}

// Planner 依据确定性规则计算分片与副本位置。
//
// 规则（N = 后端总数，后端按名字排序后编号 0..N-1）：
//
//	base(i)      = (logicalBlock + i) mod N           // 第 i 个 chunk 的条带起点
//	backend(i,r) = backends[(base(i) + r) mod N]      // 第 r 个副本
//
// 性质：
//   - 完全确定：相同输入（后端集合、逻辑块号、数据）永远得到相同布局，可复现；
//   - 条带化：相邻 chunk 的条带起点轮转，负载均匀分散到所有后端；
//   - 同一 chunk 的 r 个副本落在不同后端上（r <= N 时下标互不相同）。
type Planner struct{}

func NewPlanner() *Planner { return &Planner{} }

// Plan 计算布局。healthy 用于在规划阶段排除不可用后端：
// 若可用后端数量 < replicas，立即以 ErrInsufficientBackends 失败，
// 绝不静默降低副本数。
func (pl *Planner) Plan(backends []string, healthy func(string) bool, logicalBlock uint64,
	data []byte, chunkSize, replicas int) (*PlacementPlan, error) {

	if chunkSize <= 0 {
		return nil, fmt.Errorf("%w: chunk-size must be positive, got %d", ErrInvalidArgument, chunkSize)
	}
	if replicas <= 0 {
		return nil, fmt.Errorf("%w: replicas must be positive, got %d", ErrInvalidArgument, replicas)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: empty write is not allowed", ErrInvalidArgument)
	}

	sorted := append([]string(nil), backends...)
	sort.Strings(sorted)
	n := len(sorted)
	if n < replicas {
		return nil, fmt.Errorf("%w: need %d replicas but only %d backends configured",
			ErrInsufficientBackends, replicas, n)
	}

	// 规划阶段可用性校验：可用后端池不足则直接失败，并列出不可用后端。
	var down []string
	alive := make([]string, 0, n)
	for _, name := range sorted {
		if healthy == nil || healthy(name) {
			alive = append(alive, name)
		} else {
			down = append(down, name)
		}
	}
	if len(alive) < replicas {
		return nil, fmt.Errorf("%w: need %d healthy backends, have %d/%d (unavailable=%v)",
			ErrInsufficientBackends, replicas, len(alive), n, down)
	}

	chunkCount := ChunkCount(len(data), chunkSize)
	plan := &PlacementPlan{
		LogicalBlock:  logicalBlock,
		ChunkSize:     chunkSize,
		Length:        len(data),
		Backends:      sorted,
		RequestedReps: replicas,
		Chunks:        make([][]Placement, chunkCount),
	}

	for i := 0; i < chunkCount; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := data[start:end]
		sum := Checksum(chunk)
		// 块文件名内嵌块号/chunk/副本/摘要：内容寻址，旧版本文件自动成为垃圾。
		blockID := fmt.Sprintf("b%d-c%d-%s", logicalBlock, i, sum[:16])
		base := (int(logicalBlock) + i) % n
		reps := make([]Placement, replicas)
		for r := 0; r < replicas; r++ {
			idx := (base + r) % n
			reps[r] = Placement{
				LogicalBlock: logicalBlock,
				ChunkIndex:   i,
				ReplicaIndex: r,
				Backend:      sorted[idx],
				BlockID:      blockID, // 同一 chunk 的所有副本内容相同 => 同名
				Checksum:     sum,
				Data:         append([]byte(nil), chunk...),
			}
		}
		plan.Chunks[i] = reps
	}
	return plan, nil
}

// BlockIDOf 暴露块文件名规则，供恢复/GC 等模块复用。
func BlockIDOf(logicalBlock uint64, chunkIndex int, checksum string) string {
	return fmt.Sprintf("b%d-c%d-%s", logicalBlock, chunkIndex, checksum[:16])
}
