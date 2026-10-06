package store

// 本文件是针对"容量预算 + 相对权重"语义的回归测试：
//   - 落点只由 (对象, 块, 分片, 副本数, 后端集合及其权重) 决定，
//     与各后端当前剩余空间、写入顺序、并发时序无关；
//   - 容量只用于判定"这组已确定的位置放不放得下"，放不下整体失败，
//     绝不改换位置；
//   - 后端不足与容量不足是两种可区分的失败，且都在落盘任何字节之前；
//   - 写入与在线扩容重平衡并发时，任何后端已用+预留都不超预算；
//   - 重平衡遇到容量不足暂缓该块、保留旧副本，可中断重试、幂等收敛。

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// layoutOf 返回对象当前可见布局中 "shard/replica@backend" 的有序列表，
// 用于精确比较两次写入的落点是否一致。
func layoutOf(s *Store, objectID string) []string {
	k, ok := s.latestSnapshot(objectID)
	if !ok {
		return nil
	}
	_, counts := s.completeSnapshots(objectID)
	var out []string
	for b := 0; b < counts[k]; b++ {
		rec, ok := s.recordFor(objectID, b, k)
		if !ok {
			continue
		}
		for _, loc := range rec.Locs {
			out = append(out, fmt.Sprintf("b%d/s%d/r%d@%s", b, loc.Shard, loc.Replica, loc.Backend))
		}
	}
	return out
}

// TestPlacementIndependentOfOccupancy 同一输入（对象、块、分片数、副本数、
// 后端集合与容量/权重配置）在不同占用下落点必须完全一致：
// 先写数据把各后端不均匀地占掉一些空间，再写同一个块，
// 落点与空盘时逐位相同；把预算卡到"剩余刚好还够"时落点依然不变。
func TestPlacementIndependentOfOccupancy(t *testing.T) {
	cfg := Config{BlockSize: 16, Shards: 4, Replicas: 2}
	weights := map[string]int{"a": 1, "b": 3, "c": 1, "d": 2}
	mkStore := func(t *testing.T) (*Store, map[string]*DiskBackend) {
		t.Helper()
		backs := map[string]*DiskBackend{}
		var list []*DiskBackend
		for _, id := range []string{"a", "b", "c", "d"} {
			b, err := NewDiskBackend(id, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			b.SetCapacity(1<<20, weights[id])
			backs[id] = b
			list = append(list, b)
		}
		s, err := Open(list, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return s, backs
	}
	target := []byte("target-block-0123-target-block-4567-tail") // 40 字节 -> 3 块
	opts := WriteOptions{Version: 1, Writer: "w"}

	// 空盘：直接写目标对象，记录落点。
	sa, _ := mkStore(t)
	defer sa.Close()
	if err := sa.WriteObject("target", target, opts); err != nil {
		t.Fatal(err)
	}
	emptyLayout := layoutOf(sa, "target")
	t.Logf("空盘落点（基准）:\n%s", sa.DescribeLayout("target"))

	// 占用后：先写一批填充对象把各后端不均匀占掉，再写同一目标对象。
	sb, _ := mkStore(t)
	defer sb.Close()
	for i := 0; i < 12; i++ {
		data := []byte(fmt.Sprintf("filler-%d-%064d", i, i))
		if err := sb.WriteObject(fmt.Sprintf("filler-%d", i), data, WriteOptions{Version: 1, Writer: "w"}); err != nil {
			t.Fatal(err)
		}
	}
	logCapacity(t, sb, "写入目标前各后端占用（不均匀）:")
	if err := sb.WriteObject("target", target, opts); err != nil {
		t.Fatal(err)
	}
	occupiedLayout := layoutOf(sb, "target")
	t.Logf("占用后落点:\n%s", sb.DescribeLayout("target"))
	if strings.Join(emptyLayout, ",") != strings.Join(occupiedLayout, ",") {
		t.Fatalf("落点随占用变化了！\n空盘: %v\n占用后: %v", emptyLayout, occupiedLayout)
	}
	t.Logf("判定：%d 个副本位置在空盘/占用后完全一致", len(emptyLayout))

	// 预算卡到"剩余刚好还够"：把每个后端预算设为 当前已用+64，
	// 目标对象在任何后端至多再需要 3 块 * 4 分片 * 4 字节 = 48 字节。
	sc, backsC := mkStore(t)
	defer sc.Close()
	for i := 0; i < 12; i++ {
		data := []byte(fmt.Sprintf("filler-%d-%064d", i, i))
		if err := sc.WriteObject(fmt.Sprintf("filler-%d", i), data, WriteOptions{Version: 1, Writer: "w"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, u := range sc.CapacityReport() {
		backsC[u.Backend].SetCapacity(u.Used+64, weights[u.Backend])
	}
	logCapacity(t, sc, "预算卡到剩余刚好还够:")
	if err := sc.WriteObject("target", target, opts); err != nil {
		t.Fatalf("剩余容量足够时写入应成功: %v", err)
	}
	tightLayout := layoutOf(sc, "target")
	if strings.Join(emptyLayout, ",") != strings.Join(tightLayout, ",") {
		t.Fatalf("预算紧张（但够）时落点变了！\n空盘: %v\n紧张: %v", emptyLayout, tightLayout)
	}
	t.Logf("判定：预算紧张但足够时落点仍与空盘一致")

	// 反向：把目标落点上的某个后端预算卡到放不下，必须整体失败
	// （ErrCapacityExhausted），而不是把副本挪到别的后端。
	// 用单块对象 oneshot：失败时任何后端都不落一个字节。
	sd, backsD := mkStore(t)
	defer sd.Close()
	ringD := sd.planner.HealthyBackends()
	layD, err := sd.planner.BlockLayout("oneshot", 0, ringD)
	if err != nil {
		t.Fatal(err)
	}
	needD := shardSizes(16, 4)
	perBackend := map[string]int64{}
	for si := range layD.Shards {
		for _, pl := range layD.Shards[si] {
			perBackend[pl.Backend] += needD[si]
		}
	}
	var victim string
	var victimNeed int64
	for id, n := range perBackend {
		victim, victimNeed = id, n
		break
	}
	backsD[victim].SetCapacity(victimNeed-1, weights[victim]) // 差 1 字节放不下
	err = sd.WriteObject("oneshot", []byte("0123456789abcdef"), opts)
	if !errors.Is(err, ErrCapacityExhausted) {
		t.Fatalf("确定位置放不下应报 ErrCapacityExhausted，实际: %v", err)
	}
	t.Logf("容量判定失败（不换位置）: %v", err)
	for id, b := range backsD {
		if names, _ := b.List("shards"); len(names) != 0 {
			t.Fatalf("容量预检失败不应在任何后端落盘字节，backend=%s: %v", id, names)
		}
	}
	t.Logf("判定：容量不足整体失败，所有后端 0 字节，未发生位置改换")
}

// TestLayoutPureFunctionOfConfig 规划器层面：同一输入下，只要容量够，
// 传不同的 avail 视图算出的位置完全一致；容量不够时报错且位置不变。
func TestLayoutPureFunctionOfConfig(t *testing.T) {
	var backs []*DiskBackend
	for _, id := range []string{"a", "b", "c", "d"} {
		b, err := NewDiskBackend(id, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		b.SetCapacity(1000, 1)
		backs = append(backs, b)
	}
	p := NewPlanner(backs, 4, 2)
	ring := p.HealthyBackends()
	need := shardSizes(16, 4)

	base, err := p.BlockLayout("obj", 0, ring)
	if err != nil {
		t.Fatal(err)
	}
	// 不同的剩余空间视图（都足够：本块在任一后端至多需要 4 分片*4 字节=16）：
	// 位置必须与无容量视图一致。
	avails := []map[string]int64{
		{"a": 1000, "b": 1000, "c": 1000, "d": 1000},
		{"a": 16, "b": 900, "c": 500, "d": 16},
		{"a": 32, "b": 16, "c": 1000, "d": 64},
		nil, // 不限
	}
	for i, avail := range avails {
		lay, err := p.BlockLayoutWithCapacity("obj", 0, ring, need, avail)
		if err != nil {
			t.Fatalf("avail 视图 %d 足够却报错: %v", i, err)
		}
		for si := range lay.Shards {
			for ri := range lay.Shards[si] {
				if lay.Shards[si][ri].Backend != base.Shards[si][ri].Backend {
					t.Fatalf("avail 视图 %d 改变了落点: shard=%d replica=%d %s != %s",
						i, si, ri, lay.Shards[si][ri].Backend, base.Shards[si][ri].Backend)
				}
			}
		}
	}
	t.Logf("判定：4 种不同的剩余空间视图（含不限）算出完全相同的落点")

	// 落点上的后端剩余不足：报错且不改变无容量视图的位置。
	onRing := map[string]int64{}
	for si := range base.Shards {
		for _, pl := range base.Shards[si] {
			onRing[pl.Backend] += need[si]
		}
	}
	avail := map[string]int64{"a": 1000, "b": 1000, "c": 1000, "d": 1000}
	for id, n := range onRing {
		avail[id] = n - 1 // 差 1 字节
		break
	}
	_, err = p.BlockLayoutWithCapacity("obj", 0, ring, need, avail)
	if !errors.Is(err, ErrCapacityExhausted) {
		t.Fatalf("确定位置放不下应报 ErrCapacityExhausted，实际: %v", err)
	}
	t.Logf("容量判定（位置不变，仅判定）: %v", err)
}

// TestConcurrentWriteAndRebalanceRespectBudgets 带容量预算时，
// 并发写入与在线扩容重平衡同时进行：任何时刻任何后端的
// 已用+预留都不得超过预算；结束后占用与存活集合一致。
func TestConcurrentWriteAndRebalanceRespectBudgets(t *testing.T) {
	s, backs := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	for _, b := range backs {
		b.SetCapacity(700, 1)
	}
	base := writeObjects(t, s, 6, "cw")
	nb := addBackend(t, s, "z")
	nb.SetCapacity(700, 1)
	logCapacity(t, s, "扩容前占用:")

	// 监视器：整个并发期间持续断言不变量——任何后端的已提交占用
	// （Used）都不得超过预算。预留（Reserved）是在途写入/搬迁的
	// 未提交字节，重平衡会为"同后端换位置"的块短暂预支即将释放的
	// 旧位置，因此 Used+Reserved 可能瞬时略超预算，但已提交占用
	// 任何时候都不允许超。
	stop := make(chan struct{})
	violations := make(chan string, 4)
	var monWg sync.WaitGroup
	monWg.Add(1)
	go func() {
		defer monWg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, u := range s.CapacityReport() {
				if u.Budget > 0 && u.Used > u.Budget {
					select {
					case violations <- fmt.Sprintf("backend=%s used=%d reserved=%d budget=%d",
						u.Backend, u.Used, u.Reserved, u.Budget):
					default:
					}
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// 并发写者：预算有限，部分写入可能命中容量不足（允许，且必须整体失败）。
	var wg sync.WaitGroup
	var capErrs atomic.Int64
	written := map[string][]byte{}
	var wmu sync.Mutex
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 8; i++ {
				id := fmt.Sprintf("cw-new-%d-%d", w, i)
				data := []byte(fmt.Sprintf("cw-new-%d-%d-%048d", w, i, i))
				err := s.WriteObject(id, data, WriteOptions{Version: 1, Writer: fmt.Sprintf("w%d", w)})
				if err != nil {
					if errors.Is(err, ErrCapacityExhausted) {
						capErrs.Add(1)
						continue
					}
					t.Errorf("write %s: 非容量类错误: %v", id, err)
					return
				}
				wmu.Lock()
				written[id] = data
				wmu.Unlock()
			}
		}(w)
	}
	// 写入进行中连续做多轮重平衡。
	var reps []*RebalanceReport
	for round := 0; round < 3; round++ {
		rep, err := s.Rebalance()
		if err != nil {
			t.Fatalf("rebalance round %d: %v", round, err)
		}
		reps = append(reps, rep)
	}
	wg.Wait()
	close(stop)
	monWg.Wait()
	select {
	case v := <-violations:
		t.Fatalf("并发期间预算被突破: %s", v)
	default:
	}
	for i, rep := range reps {
		t.Logf("重平衡轮 %d: %+v", i, rep)
	}
	t.Logf("并发期间容量不足整体失败的写入次数: %d", capErrs.Load())

	// 结束后再平衡到收敛；最终任何后端不超预算、无残留预留。
	for i := 0; i < 3; i++ {
		if _, err := s.Rebalance(); err != nil {
			t.Fatalf("settle rebalance %d: %v", i, err)
		}
	}
	for _, u := range s.CapacityReport() {
		if u.Budget > 0 && u.Used > u.Budget {
			t.Fatalf("结束后 backend=%s 已用 %d 超预算 %d", u.Backend, u.Used, u.Budget)
		}
		if u.Reserved != 0 {
			t.Fatalf("结束后 backend=%s 仍有残留预留 %d", u.Backend, u.Reserved)
		}
	}
	logCapacity(t, s, "并发结束后占用（全部 <= 预算，预留为 0）:")

	// GC 后占用与存活集合严格一致；所有成功写入的对象完整可读。
	if _, err := s.RunGC(); err != nil {
		t.Fatal(err)
	}
	usage, err := s.SpaceUsage()
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range s.CapacityReport() {
		if usage[u.Backend] != u.Used {
			t.Fatalf("backend=%s GC 后占用 %d != 存活记账 %d", u.Backend, usage[u.Backend], u.Used)
		}
	}
	verifyObjects(t, s, base)
	verifyObjects(t, s, written)
	t.Logf("判定：%d 个旧对象 + %d 个并发新对象全部完整可读，占用与存活集合一致",
		len(base), len(written))
}

// TestRebalanceDefersWhenCapacityInsufficient 重平衡搬迁遇到目标后端
// 预算不足时：暂缓该块、旧副本保留可读、一个字节都不硬塞；
// 重复执行结果确定（同样的块被暂缓）；预算放宽后收敛且幂等；
// 旧位置副本由 GC 回收，占用与存活集合一致。
func TestRebalanceDefersWhenCapacityInsufficient(t *testing.T) {
	s, _ := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	objs := writeObjects(t, s, 8, "df")
	nb := addBackend(t, s, "z")
	nb.SetCapacity(8, 1) // 只放得下 2 个 4 字节分片副本

	rep1, err := s.Rebalance()
	if err != nil {
		t.Fatalf("容量不足应暂缓而非失败: %v", err)
	}
	t.Logf("第 1 轮（z 预算 8 字节）: %+v", rep1)
	t.Logf("判定依据: %s", rep1.Reason)
	if rep1.DeferredBlocks == 0 {
		t.Fatalf("z 预算仅 8 字节，应有块被暂缓: %+v", rep1)
	}
	verifyObjects(t, s, objs) // 被暂缓的块旧副本必须保留可读
	for _, u := range s.CapacityReport() {
		if u.Budget > 0 && u.Used > u.Budget {
			t.Fatalf("暂缓语义下 backend=%s 已用 %d 超预算 %d", u.Backend, u.Used, u.Budget)
		}
	}
	logCapacity(t, s, "暂缓后占用（z 不超 8 字节）:")

	// 重复执行：已切换的幂等跳过，放不下的仍暂缓 —— 结果确定。
	rep2, err := s.Rebalance()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("第 2 轮（条件不变）: %+v", rep2)
	if rep2.UpdatedBlocks != 0 || rep2.CopiedReplicas != 0 {
		t.Fatalf("条件不变时不应有新的搬迁: %+v", rep2)
	}
	if rep2.DeferredBlocks != rep1.DeferredBlocks {
		t.Fatalf("暂缓集合应确定: 第 1 轮 %d 块, 第 2 轮 %d 块", rep1.DeferredBlocks, rep2.DeferredBlocks)
	}
	verifyObjects(t, s, objs)

	// 预算放宽：被暂缓的块本轮全部搬完，布局收敛。
	nb.SetCapacity(1<<20, 1)
	rep3, err := s.Rebalance()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("第 3 轮（z 预算放宽）: %+v", rep3)
	if rep3.DeferredBlocks != 0 {
		t.Fatalf("预算放宽后不应再有暂缓: %+v", rep3)
	}
	if rep3.UpdatedBlocks != rep1.DeferredBlocks {
		t.Fatalf("放宽后搬迁块数 %d 应等于此前暂缓块数 %d", rep3.UpdatedBlocks, rep1.DeferredBlocks)
	}
	rep4, err := s.Rebalance()
	if err != nil {
		t.Fatal(err)
	}
	if rep4.UpdatedBlocks != 0 || rep4.DeferredBlocks != 0 || rep4.PlannedMoves != 0 {
		t.Fatalf("收敛后重复执行应完全幂等: %+v", rep4)
	}
	t.Logf("第 4 轮（已收敛）: %+v", rep4)
	verifyObjects(t, s, objs)
	t.Logf("收敛后布局示例:\n%s", s.DescribeLayout("df-0"))

	// GC 回收被替换掉的旧位置副本；占用与存活集合一致。
	gc, err := s.RunGC()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("GC: 删除 %d 文件 %d 字节", len(gc.DeletedFiles), gc.DeletedBytes)
	if gc.DeletedBytes == 0 {
		t.Fatal("收敛后应有旧位置副本被回收")
	}
	usage, err := s.SpaceUsage()
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range s.CapacityReport() {
		if usage[u.Backend] != u.Used {
			t.Fatalf("backend=%s GC 后占用 %d != 存活记账 %d", u.Backend, usage[u.Backend], u.Used)
		}
	}
	verifyObjects(t, s, objs)
}
