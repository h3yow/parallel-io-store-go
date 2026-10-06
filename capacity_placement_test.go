package store

// 本文件的测试覆盖"容量预算 vs 确定性落点"的核心回归：
//   - 落点只由 (对象, 块, 分片, 副本数, 后端配置含权重) 决定，
//     与各后端当前剩余空间、写入次数、并发顺序无关；
//   - 容量只用于判定已确定的位置放不放得下：放不下就整体失败
//     （ErrCapacityExhausted，一个字节都不落），绝不改换落点；
//   - 带预算的写入与在线扩容重平衡并发时，任何后端都不超预算；
//   - 重平衡遇容量不足暂缓并保留旧副本，条件允许后重试收敛，幂等。

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// committedLayout 返回对象当前可见快照的 "shard/replica -> backend" 落点表。
func committedLayout(t *testing.T, s *Store, objectID string) map[string]string {
	t.Helper()
	k, ok := s.latestSnapshot(objectID)
	if !ok {
		t.Fatalf("object %s: no complete snapshot", objectID)
	}
	_, counts := s.completeSnapshots(objectID)
	out := map[string]string{}
	for b := 0; b < counts[k]; b++ {
		rec, ok := s.recordFor(objectID, b, k)
		if !ok {
			t.Fatalf("object %s block %d: missing record", objectID, b)
		}
		for _, loc := range rec.Locs {
			out[fmt.Sprintf("b%d/s%d/r%d", b, loc.Shard, loc.Replica)] = loc.Backend
		}
	}
	return out
}

// TestPlacementIndependentOfOccupancy 同一组输入（对象、块、分片、副本数、
// 后端及其容量/权重配置）无论各后端当前被占掉多少空间，落点必须完全一致；
// 容量只判定放不放得下，绝不改变落点。
func TestPlacementIndependentOfOccupancy(t *testing.T) {
	cfg := Config{BlockSize: 16, Shards: 4, Replicas: 2}
	// 场景 A：空盘直接写 probe，记录落点。
	sA, backsA := newTestStore(t, 4, cfg)
	for _, b := range backsA {
		b.SetCapacity(1<<20, 1)
	}
	probe := []byte("0123456789abcdef")
	if err := sA.WriteObject("probe", probe, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	emptyLayout := committedLayout(t, sA, "probe")
	t.Logf("空盘落点: %v", emptyLayout)
	t.Logf("空盘布局说明:\n%s", sA.DescribeLayout("probe"))

	// 场景 B：先写一堆填充数据把各后端占掉不同空间，再写同一个 probe。
	sB, backsB := newTestStore(t, 4, cfg)
	for _, b := range backsB {
		b.SetCapacity(1<<20, 1)
	}
	for i := 0; i < 9; i++ {
		filler := []byte(fmt.Sprintf("filler-%d-%040d", i, i))
		if err := sB.WriteObject(fmt.Sprintf("filler-%d", i), filler, WriteOptions{Version: 1, Writer: "w"}); err != nil {
			t.Fatal(err)
		}
	}
	logCapacity(t, sB, "填充后占用:")
	if err := sB.WriteObject("probe", probe, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	busyLayout := committedLayout(t, sB, "probe")
	t.Logf("占用后落点: %v", busyLayout)
	for k, want := range emptyLayout {
		if busyLayout[k] != want {
			t.Fatalf("落点随占用变化: %s 空盘=%s 占用后=%s", k, want, busyLayout[k])
		}
	}
	t.Logf("判定：%d 个副本位置在空盘/占用后完全一致", len(emptyLayout))

	// 场景 C：规划器层面 —— 同一输入配不同的剩余容量视图，
	// 结果要么逐位相同、要么整体 ErrCapacityExhausted，绝不出现"换位置"。
	p := sA.planner
	ring := p.HealthyBackends()
	need := shardSizes(len(probe), cfg.Shards)
	base, err := p.BlockLayoutWithCapacity("probe", 0, ring, need, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, avail := range []map[string]int64{
		{"a": 1 << 20, "b": 1 << 20, "c": 1 << 20, "d": 1 << 20},
		{"a": 3, "b": 1 << 20, "c": 7, "d": 1 << 20},
		{"a": 0, "b": 0, "c": 0, "d": 0},
		{"a": -1, "b": 100, "c": -1, "d": 4},
	} {
		lay, err := p.BlockLayoutWithCapacity("probe", 0, ring, need, avail)
		if err != nil {
			if !errors.Is(err, ErrCapacityExhausted) {
				t.Fatalf("avail=%v: 期望成功或 ErrCapacityExhausted，实际: %v", avail, err)
			}
			t.Logf("avail=%v -> 整体失败(容量)，未改换落点", avail)
			continue
		}
		for si := range lay.Shards {
			for ri := range lay.Shards[si] {
				if lay.Shards[si][ri].Backend != base.Shards[si][ri].Backend {
					t.Fatalf("avail=%v 时落点改变: s%d/r%d %s -> %s",
						avail, si, ri, base.Shards[si][ri].Backend, lay.Shards[si][ri].Backend)
				}
			}
		}
		t.Logf("avail=%v -> 落点与基准逐位一致", avail)
	}
}

// TestFullBackendFailsInsteadOfRelocating 某个"已确定的目标后端"剩余空间
// 不足时，写入必须整体失败（ErrCapacityExhausted、一个字节都不落），
// 绝不把副本改落到别的后端；预算放宽后同一对象落点与空盘时完全一致。
func TestFullBackendFailsInsteadOfRelocating(t *testing.T) {
	cfg := Config{BlockSize: 16, Shards: 4, Replicas: 2}
	probe := []byte("0123456789abcdef")

	// 先用一个临时实例算出 probe 在空盘下的确定性落点。
	s0, b0 := newTestStore(t, 4, cfg)
	for _, b := range b0 {
		b.SetCapacity(1<<20, 1)
	}
	if err := s0.WriteObject("probe", probe, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	want := committedLayout(t, s0, "probe")
	t.Logf("空盘确定性落点: %v", want)

	// 正式场景：把其中一个目标后端的预算卡到放不下一个分片。
	s, backs := newTestStore(t, 4, cfg)
	for _, b := range backs {
		b.SetCapacity(1<<20, 1)
	}
	// 任选一个 probe 的目标后端（取 s0/r0 的落点），预算卡到 1 字节。
	victim := want["b0/s0/r0"]
	s.Backend(victim).SetCapacity(1, 1)
	t.Logf("把目标后端 %s 的预算卡到 1 字节（一个分片需 4 字节）", victim)

	err := s.WriteObject("probe", probe, WriteOptions{Version: 1, Writer: "w"})
	if !errors.Is(err, ErrCapacityExhausted) {
		t.Fatalf("目标后端放不下应整体报 ErrCapacityExhausted，实际: %v", err)
	}
	if errors.Is(err, ErrReplicaShortage) {
		t.Fatalf("容量不足不应被误判为后端不足: %v", err)
	}
	t.Logf("判定: %v", err)
	usage, uerr := s.SpaceUsage()
	if uerr != nil {
		t.Fatal(uerr)
	}
	for id, n := range usage {
		if n != 0 {
			t.Fatalf("容量失败不应落盘任何字节，backend=%s 有 %d 字节", id, n)
		}
	}
	t.Logf("判定：容量失败时各后端占用全为 0，一个字节都未落盘")
	// 放宽预算后重写：必须成功，且落点与空盘时逐位一致。
	s.Backend(victim).SetCapacity(1<<20, 1)
	if err := s.WriteObject("probe", probe, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	got := committedLayout(t, s, "probe")
	for k, w := range want {
		if got[k] != w {
			t.Fatalf("放宽预算后落点与空盘不一致: %s 空盘=%s 实际=%s", k, w, got[k])
		}
	}
	t.Logf("判定：预算放宽后落点与空盘逐位一致（%d 个副本位置）", len(got))
}

// TestConcurrentWritesSameLayoutRegardlessOfOrder 同一批对象，顺序写与
// 高并发写（调度顺序随机）最终提交的落点必须逐位一致 —— 落点与写入
// 次数、并发顺序、各后端被占用的时序无关。
func TestConcurrentWritesSameLayoutRegardlessOfOrder(t *testing.T) {
	cfg := Config{BlockSize: 16, Shards: 4, Replicas: 2}
	const nObjs = 12
	objs := map[string][]byte{}
	for i := 0; i < nObjs; i++ {
		objs[fmt.Sprintf("obj-%02d", i)] = []byte(fmt.Sprintf("payload-%02d-%024d", i, i))
	}

	// 运行 1：严格顺序写。
	s1, b1 := newTestStore(t, 4, cfg)
	for _, b := range b1 {
		b.SetCapacity(1<<20, 1)
	}
	for i := 0; i < nObjs; i++ {
		id := fmt.Sprintf("obj-%02d", i)
		if err := s1.WriteObject(id, objs[id], WriteOptions{Version: 1, Writer: "w"}); err != nil {
			t.Fatal(err)
		}
	}

	// 运行 2：全部对象并发写（goroutine 调度顺序不可控）。
	s2, b2 := newTestStore(t, 4, cfg)
	for _, b := range b2 {
		b.SetCapacity(1<<20, 1)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, nObjs)
	for i := 0; i < nObjs; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("obj-%02d", i)
			if err := s2.WriteObject(id, objs[id], WriteOptions{Version: 1, Writer: "w"}); err != nil {
				errCh <- fmt.Errorf("concurrent write %s: %w", id, err)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Fatal(e)
	}

	// 逐对象比对落点。
	for id := range objs {
		l1 := committedLayout(t, s1, id)
		l2 := committedLayout(t, s2, id)
		for k, want := range l1 {
			if l2[k] != want {
				t.Fatalf("对象 %s 落点随写入顺序变化: %s 顺序=%s 并发=%s", id, k, want, l2[k])
			}
		}
	}
	t.Logf("判定：%d 个对象在顺序写与并发写下落点逐位一致", nObjs)
	t.Logf("并发写布局示例:\n%s", s2.DescribeLayout("obj-00"))
}

// TestRebalanceDefersWhenTargetBudgetTooSmall 扩容重平衡时，若某块的
// 目标位置预算内放不下，必须暂缓该块迁移（旧副本保留、照常可读），
// 而不是硬塞写爆后端或整体报错；预算放宽后再次重平衡收敛，重复执行幂等。
func TestRebalanceDefersWhenTargetBudgetTooSmall(t *testing.T) {
	cfg := Config{BlockSize: 16, Shards: 4, Replicas: 2}
	s, backs := newTestStore(t, 4, cfg)
	for _, b := range backs {
		b.SetCapacity(1<<20, 1)
	}
	objs := writeObjects(t, s, 6, "def")

	// 新后端预算只够 4 字节（一个分片副本）：凡有副本要迁入 z 的块都暂缓。
	z := addBackend(t, s, "z")
	z.SetCapacity(4, 1)
	rep1, err := s.Rebalance()
	if err != nil {
		t.Fatalf("容量不足应暂缓而非报错: %v", err)
	}
	t.Logf("第一轮（z 预算 4 字节）: %+v", rep1)
	t.Logf("判定依据: %s", rep1.Reason)
	if rep1.DeferredBlocks == 0 {
		t.Fatalf("z 预算仅 4 字节时应有块被暂缓: %+v", rep1)
	}
	// 旧副本保留、数据完整可读。
	verifyObjects(t, s, objs)
	// 任何后端都没有超预算。
	for _, u := range s.CapacityReport() {
		if u.Budget > 0 && u.Used > u.Budget {
			t.Fatalf("backend=%s 已用 %d 超过预算 %d", u.Backend, u.Used, u.Budget)
		}
	}
	logCapacity(t, s, "暂缓后占用:")
	// 重复执行：结论稳定（同样的块继续暂缓，不产生多余副本）。
	rep1b, err := s.Rebalance()
	if err != nil {
		t.Fatal(err)
	}
	if rep1b.UpdatedBlocks != 0 || rep1b.DeferredBlocks != rep1.DeferredBlocks {
		t.Fatalf("预算不变时重复重平衡结论应稳定: 第一轮 %+v 第二轮 %+v", rep1, rep1b)
	}
	verifyObjects(t, s, objs)

	// 预算放宽：暂缓的块应完成迁移并收敛。
	z.SetCapacity(1<<20, 1)
	rep2, err := s.Rebalance()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("第二轮（z 预算放宽）: %+v", rep2)
	if rep2.DeferredBlocks != 0 {
		t.Fatalf("预算放宽后不应再有暂缓: %+v", rep2)
	}
	rep3, err := s.Rebalance()
	if err != nil {
		t.Fatal(err)
	}
	if rep3.UpdatedBlocks != 0 || rep3.PlannedMoves != 0 || rep3.DeferredBlocks != 0 {
		t.Fatalf("收敛后重复重平衡应完全幂等: %+v", rep3)
	}
	verifyObjects(t, s, objs)
	// GC 回收被取代的旧位置副本；占用与存活集合严格一致。
	gc, err := s.RunGC()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("GC: 删除 %d 文件 %d 字节", len(gc.DeletedFiles), gc.DeletedBytes)
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
	t.Logf("判定：暂缓->放宽->收敛->GC 全链路通过，最终布局:\n%s", s.DescribeLayout("def-0"))
}

// TestRebalanceConcurrentWritesNeverExceedBudget 带容量预算时，并发写入与
// 在线扩容重平衡同时进行：任何时刻任何后端的 已用+预留 都不得超过预算；
// 写不下的写入整体失败（ErrCapacityExhausted）但绝不把后端写爆；
// 结束后所有已提交对象可读、占用与存活集合一致。
func TestRebalanceConcurrentWritesNeverExceedBudget(t *testing.T) {
	cfg := Config{BlockSize: 16, Shards: 4, Replicas: 2}
	s, backs := newTestStore(t, 4, cfg)
	const budget = 320 // 每后端 320 字节：能装一批，但写太满会触发容量失败
	for _, b := range backs {
		b.SetCapacity(budget, 1)
	}
	base := writeObjects(t, s, 4, "base")
	z := addBackend(t, s, "z")
	z.SetCapacity(budget, 1)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	errCh := make(chan error, 128)
	// 监控协程：高频抽查 已用/已用+预留 超预算即报错。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, u := range s.CapacityReport() {
				if u.Budget > 0 {
					if u.Used > u.Budget {
						errCh <- fmt.Errorf("backend=%s used=%d exceeds budget=%d", u.Backend, u.Used, u.Budget)
					}
					if u.Used+u.Reserved > u.Budget {
						errCh <- fmt.Errorf("backend=%s used+reserved=%d+%d exceeds budget=%d", u.Backend, u.Used, u.Reserved, u.Budget)
					}
				}
			}
		}
	}()
	// 并发写者：写全新对象；容量失败是合法结果（整体失败），其它错误不是。
	var wmu sync.Mutex
	newObjs := map[string][]byte{}
	var capacityFails int
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				id := fmt.Sprintf("conc-%d-%d", w, i)
				data := []byte(fmt.Sprintf("conc-%d-%d-%040d", w, i, i))
				err := s.WriteObject(id, data, WriteOptions{Version: 1, Writer: fmt.Sprintf("w%d", w)})
				if err != nil {
					if errors.Is(err, ErrCapacityExhausted) {
						wmu.Lock()
						capacityFails++
						wmu.Unlock()
						continue
					}
					errCh <- fmt.Errorf("write %s: unexpected error: %w", id, err)
					return
				}
				wmu.Lock()
				newObjs[id] = data
				wmu.Unlock()
			}
		}(w)
	}
	// 主协程：并发期间连续做多轮重平衡，直到收敛或仅剩容量暂缓。
	var lastRep *RebalanceReport
	for round := 0; round < 6; round++ {
		rep, err := s.Rebalance()
		if err != nil {
			errCh <- fmt.Errorf("rebalance round %d: %w", round, err)
			break
		}
		lastRep = rep
		if rep.UpdatedBlocks == 0 && rep.DeferredBlocks == 0 {
			break
		}
	}
	t.Logf("末轮重平衡: %+v", lastRep)
	close(stop)
	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Fatal(e)
	}
	t.Logf("并发期间容量失败（合法整体失败）次数: %d, 成功写入新对象: %d", capacityFails, len(newObjs))
	// 结束后：所有已提交对象完整可读。
	verifyObjects(t, s, base)
	verifyObjects(t, s, newObjs)
	// GC 之后：磁盘占用与存活记账一致，且都不超预算。
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
		if u.Budget > 0 && u.Used > u.Budget {
			t.Fatalf("backend=%s 结束后已用 %d 超过预算 %d", u.Backend, u.Used, u.Budget)
		}
	}
	logCapacity(t, s, "并发+重平衡结束后占用（均不超预算）:")
	t.Logf("判定：全程无任何后端超预算，数据完整一致")
}

// TestRebalanceInterruptWithBudgetRetryIdempotent 带容量预算的重平衡被
// 中断（目标后端写故障）后：已提交数据可读、任何后端不超预算；
// 故障恢复后重试收敛到同一布局，重复执行幂等，GC 后占用与存活集合一致。
func TestRebalanceInterruptWithBudgetRetryIdempotent(t *testing.T) {
	cfg := Config{BlockSize: 16, Shards: 4, Replicas: 2}
	s, backs := newTestStore(t, 4, cfg)
	for _, b := range backs {
		b.SetCapacity(1<<20, 1)
	}
	objs := writeObjects(t, s, 6, "ibk")
	z := addBackend(t, s, "z")
	z.SetCapacity(1<<20, 1)

	assertWithinBudget := func(phase string) {
		t.Helper()
		for _, u := range s.CapacityReport() {
			if u.Budget > 0 && u.Used+u.Reserved > u.Budget {
				t.Fatalf("%s: backend=%s used+reserved=%d+%d 超预算 %d",
					phase, u.Backend, u.Used, u.Reserved, u.Budget)
			}
		}
	}

	// 注入"复制中途目标后端写失败"模拟中断。
	z.InjectFault(FaultWriteFail, 1)
	rep1, err := s.Rebalance()
	if err == nil {
		t.Fatal("注入写故障时重平衡应失败")
	}
	t.Logf("中断的重平衡: err=%v", err)
	t.Logf("中断时部分报告: %+v", rep1)
	verifyObjects(t, s, objs) // 中断不影响已提交数据
	assertWithinBudget("中断后")

	// 故障恢复后重试：收敛；再重复执行幂等。
	z.ResetFaults()
	rep2, err := s.Rebalance()
	if err != nil {
		t.Fatalf("重试重平衡: %v", err)
	}
	t.Logf("重试报告: %+v", rep2)
	rep3, err := s.Rebalance()
	if err != nil {
		t.Fatal(err)
	}
	if rep3.UpdatedBlocks != 0 || rep3.PlannedMoves != 0 || rep3.DeferredBlocks != 0 {
		t.Fatalf("重试后应完全收敛且幂等: %+v", rep3)
	}
	verifyObjects(t, s, objs)
	assertWithinBudget("收敛后")

	// GC 清理中断遗留的半成品复制文件与被取代的旧位置副本。
	gc, err := s.RunGC()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("GC: 删除 %d 文件 %d 字节（含中断遗留半成品与旧位置副本）", len(gc.DeletedFiles), gc.DeletedBytes)
	if gc.DeletedBytes == 0 {
		t.Fatal("中断+重平衡后应有垃圾被回收")
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
	t.Logf("判定：中断->重试->收敛->幂等->GC 全链路通过，占用与存活集合一致")
}
