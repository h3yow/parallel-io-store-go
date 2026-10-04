package store

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"path/filepath"
	"sync"
	"testing"
)

// visibleSnapshot 读取对象当前可见内容与 CRC（平衡前后一致性判定用）。
func visibleSnapshot(t *testing.T, s *Store, ids []string) map[string]uint32 {
	t.Helper()
	out := map[string]uint32{}
	for _, id := range ids {
		data, err := s.ReadObject(id)
		if err != nil {
			t.Fatalf("read %s: %v", id, err)
		}
		out[id] = crc32.ChecksumIEEE(data)
	}
	return out
}

// assertConvergedLayout 断言每条已提交记录的位置都等于
// "用当前健康环重算的确定性布局"（收敛判定）。
func assertConvergedLayout(t *testing.T, s *Store) {
	t.Helper()
	ring := s.planner.HealthyBackends()
	for _, rec := range s.wal.Records() {
		lay, err := s.planner.BlockLayout(rec.ObjectID, rec.Block, ring)
		if err != nil {
			t.Fatalf("layout: %v", err)
		}
		want := map[[2]int]string{}
		for si := range lay.Shards {
			for _, pl := range lay.Shards[si] {
				want[[2]int{si, pl.Replica}] = pl.Backend
			}
		}
		for _, loc := range rec.Locs {
			if got := want[[2]int{loc.Shard, loc.Replica}]; got != loc.Backend {
				t.Fatalf("record %s not converged: shard=%d replica=%d at %s, want %s",
					rec.Key(), loc.Shard, loc.Replica, loc.Backend, got)
			}
		}
	}
	t.Logf("收敛判定通过：%d 条记录的位置 == 当前健康环确定性布局", len(s.wal.Records()))
}

// TestAddBackendWithoutRebalance 验证只加后端不做平衡时行为不变。
func TestAddBackendWithoutRebalance(t *testing.T) {
	s, _ := newTestStore(t, 3, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	data := []byte("add-backend-no-rebalance-data!!")
	if err := s.WriteObject("o", data, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	layoutBefore := s.DescribeLayout("o")

	nb, err := NewDiskBackend("d", filepath.Join(t.TempDir(), "d"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AddBackend(nb); err != nil {
		t.Fatal(err)
	}
	// 旧数据位置不变、读取不变、新后端上没有旧数据文件。
	if got := s.DescribeLayout("o"); got != layoutBefore {
		t.Fatalf("layout changed without rebalance:\nbefore:\n%s\nafter:\n%s", layoutBefore, got)
	}
	if n := countShardFiles(t, nb); n != 0 {
		t.Fatalf("new backend must be empty before rebalance, got %d files", n)
	}
	got, err := s.ReadObject("o")
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read after AddBackend: %v", err)
	}
	rep, err := s.RunGC()
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.DeletedFiles) != 0 {
		t.Fatalf("GC must delete nothing before rebalance: %v", rep.DeletedFiles)
	}
	t.Logf("只加后端不平衡：布局不变、新后端为空、GC 无垃圾\n%s", layoutBefore)
}

// TestRebalanceConcurrentReadWriteConsistency 验证并发读写下在线扩容
// 与重平衡的一致性：读取不失败，内容/校验和前后一致，布局收敛。
func TestRebalanceConcurrentReadWriteConsistency(t *testing.T) {
	s, _ := newTestStore(t, 3, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	var ids []string
	contents := map[string][]byte{}
	for i := 0; i < 12; i++ {
		id := fmt.Sprintf("rb-%02d", i)
		data := []byte(fmt.Sprintf("rebalance-payload-%02d-%s", i, "0123456789abcdef"))
		if err := s.WriteObject(id, data, WriteOptions{Version: 1, Writer: "w"}); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		contents[id] = data
	}
	before := visibleSnapshot(t, s, ids)
	t.Logf("平衡前布局:\n%s", s.DescribeLayout("rb-00"))

	nb, err := NewDiskBackend("d", filepath.Join(t.TempDir(), "d"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AddBackend(nb); err != nil {
		t.Fatal(err)
	}

	// 并发读者：重平衡全程读取不得失败、内容不得变化。
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var readErrs []error
	var errMu sync.Mutex
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				id := ids[g%len(ids)]
				data, err := s.ReadObject(id)
				if err != nil {
					errMu.Lock()
					readErrs = append(readErrs, fmt.Errorf("reader %d: %w", g, err))
					errMu.Unlock()
					return
				}
				if crc32.ChecksumIEEE(data) != before[id] {
					errMu.Lock()
					readErrs = append(readErrs, fmt.Errorf("reader %d: content changed for %s", g, id))
					errMu.Unlock()
					return
				}
			}
		}(g)
	}
	// 并发写者：扩容期间新写入直接落在含新后端的确定性布局上。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 4; i++ {
			id := fmt.Sprintf("rb-new-%02d", i)
			if err := s.WriteObject(id, []byte(fmt.Sprintf("new-payload-%02d-aaaaaaaaaaaaaaaa", i)),
				WriteOptions{Version: 1, Writer: "w"}); err != nil {
				errMu.Lock()
				readErrs = append(readErrs, fmt.Errorf("writer: %w", err))
				errMu.Unlock()
				return
			}
		}
	}()

	rep, err := s.Rebalance(nil)
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatalf("rebalance: %v", err)
	}
	for _, e := range readErrs {
		t.Fatalf("concurrent access failed: %v", e)
	}
	t.Logf("重平衡报告: %s", rep.Reason)
	for _, m := range rep.Moves {
		t.Logf("  搬迁 %s block=%d shard=%d r%d: %s -> %s (%dB)", m.ObjectID, m.Block, m.Shard, m.Replica, m.From, m.To, m.Bytes)
	}
	if rep.RecordsMoved == 0 {
		t.Fatalf("expected some records to move to the new backend")
	}

	// 可见内容与校验和与平衡前完全一致。
	after := visibleSnapshot(t, s, ids)
	for id, sum := range before {
		if after[id] != sum {
			t.Fatalf("object %s checksum changed: %08x -> %08x", id, sum, after[id])
		}
	}
	// 布局收敛 + 旧位置被 GC 回收后占用 == 存活集合。
	assertConvergedLayout(t, s)
	gcRep, err := s.RunGC()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("平衡后 GC: %s", gcRep.Reason)
	if len(gcRep.DeletedFiles) == 0 {
		t.Fatalf("expected old-location replicas to be reclaimed")
	}
	usage, err := s.SpaceUsage()
	if err != nil {
		t.Fatal(err)
	}
	for _, bc := range s.CapacityReport() {
		if bc.Used != usage[bc.ID] {
			t.Fatalf("backend=%s used=%d != live disk bytes=%d after GC", bc.ID, bc.Used, usage[bc.ID])
		}
	}
	t.Logf("平衡后布局:\n%s", s.DescribeLayout("rb-00"))
	logCapacity(t, s, "GC 后占用:")
}

// TestRebalanceInterruptRetryIdempotent 验证中断后重试与重复执行的幂等性。
func TestRebalanceInterruptRetryIdempotent(t *testing.T) {
	s, _ := newTestStore(t, 3, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	var ids []string
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("ir-%02d", i)
		if err := s.WriteObject(id, []byte(fmt.Sprintf("interrupt-retry-%02d-bbbbbbbbbbbbbbbb", i)),
			WriteOptions{Version: 1, Writer: "w"}); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	before := visibleSnapshot(t, s, ids)
	nb, _ := NewDiskBackend("d", filepath.Join(t.TempDir(), "d"))
	if err := s.AddBackend(nb); err != nil {
		t.Fatal(err)
	}

	// 第一轮：处理 2 条记录后模拟中断。
	boom := errors.New("simulated interruption")
	rep1, err1 := s.Rebalance(&RebalanceOptions{Hook: func(done, total int) error {
		if done >= 2 {
			return boom
		}
		return nil
	}})
	if !errors.Is(err1, boom) {
		t.Fatalf("expected interruption, got %v", err1)
	}
	t.Logf("中断轮: %s", rep1.Reason)
	// 中断后读取仍然完好。
	for id, sum := range visibleSnapshot(t, s, ids) {
		if sum != before[id] {
			t.Fatalf("content changed after interruption: %s", id)
		}
	}

	// 第二轮：重试到完成。
	rep2, err2 := s.Rebalance(nil)
	if err2 != nil {
		t.Fatalf("retry rebalance: %v", err2)
	}
	t.Logf("重试轮: %s", rep2.Reason)
	assertConvergedLayout(t, s)
	for id, sum := range visibleSnapshot(t, s, ids) {
		if sum != before[id] {
			t.Fatalf("content changed after retry: %s", id)
		}
	}

	// 第三轮：重复执行是空操作（幂等收敛）。
	rep3, err3 := s.Rebalance(nil)
	if err3 != nil {
		t.Fatal(err3)
	}
	if rep3.RecordsMoved != 0 || rep3.ReplicasCopied != 0 {
		t.Fatalf("repeated rebalance must be a no-op: %s", rep3.Reason)
	}
	t.Logf("重复轮（幂等）: %s", rep3.Reason)

	// GC 清理旧位置后，占用 == 存活集合；再 GC 为空。
	gc1, _ := s.RunGC()
	gc2, _ := s.RunGC()
	if len(gc2.DeletedFiles) != 0 {
		t.Fatalf("GC not idempotent: %v", gc2.DeletedFiles)
	}
	usage, _ := s.SpaceUsage()
	for _, bc := range s.CapacityReport() {
		if bc.Used != usage[bc.ID] {
			t.Fatalf("backend=%s used=%d != live=%d", bc.ID, bc.Used, usage[bc.ID])
		}
	}
	t.Logf("GC 第一轮回收 %d 个旧位置文件，第二轮 0（幂等）", len(gc1.DeletedFiles))
}

// TestRebalanceSurvivesRestart 验证重平衡跨"进程重启"（重放 WAL 折叠
// 重定位记录）后继续推进并收敛到同一布局。
func TestRebalanceSurvivesRestart(t *testing.T) {
	root := t.TempDir()
	mkBackends := func(ids ...string) []*DiskBackend {
		var out []*DiskBackend
		for _, id := range ids {
			b, err := NewDiskBackend(id, filepath.Join(root, id))
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, b)
		}
		return out
	}
	cfg := Config{BlockSize: 16, Shards: 4, Replicas: 2}
	s1, err := Open(mkBackends("a", "b", "c"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("rs-%02d", i)
		if err := s1.WriteObject(id, []byte(fmt.Sprintf("restart-payload-%02d-cccccccccccccccc", i)),
			WriteOptions{Version: 1, Writer: "w"}); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	before := visibleSnapshot(t, s1, ids)
	if err := s1.AddBackend(mkBackends("d")[0]); err != nil {
		t.Fatal(err)
	}
	// 只搬一部分，然后"崩溃"（直接关闭，不跑完）。
	rep1, err := s1.Rebalance(&RebalanceOptions{RecordLimit: 3})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("重启前部分平衡: %s", rep1.Reason)
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：重放 WAL（含重定位记录），继续平衡到收敛。
	s2, err := Open(mkBackends("a", "b", "c", "d"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for id, sum := range visibleSnapshot(t, s2, ids) {
		if sum != before[id] {
			t.Fatalf("content changed across restart: %s", id)
		}
	}
	rep2, err := s2.Rebalance(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("重启后续平衡: %s", rep2.Reason)
	assertConvergedLayout(t, s2)
	for id, sum := range visibleSnapshot(t, s2, ids) {
		if sum != before[id] {
			t.Fatalf("content changed after restart+rebalance: %s", id)
		}
	}
	// 再重启一次：布局与内容仍然一致（重定位记录重放幂等）。
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := Open(mkBackends("a", "b", "c", "d"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	assertConvergedLayout(t, s3)
	for id, sum := range visibleSnapshot(t, s3, ids) {
		if sum != before[id] {
			t.Fatalf("content changed after second restart: %s", id)
		}
	}
	t.Logf("两次重启后布局与内容保持一致")
}

// TestRebalanceRespectsCapacity 验证目标后端容量不足时记录整体暂缓、
// 旧位置继续服务，且原因可与其它失败区分。
func TestRebalanceRespectsCapacity(t *testing.T) {
	s, _ := newTestStore(t, 3, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	data := []byte("capacity-aware-rebalance-xxxxxxx") // 32 字节 = 2 个整块，每块需要 >1 字节
	if err := s.WriteObject("o", data, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	layoutBefore := s.DescribeLayout("o")
	nb, _ := NewDiskBackend("d", filepath.Join(t.TempDir(), "d"))
	nb.SetCapacity(1) // 新后端几乎放不下任何分片
	if err := s.AddBackend(nb); err != nil {
		t.Fatal(err)
	}
	rep, err := s.Rebalance(nil)
	if !errors.Is(err, ErrCapacityExhausted) {
		t.Fatalf("want ErrCapacityExhausted, got %v", err)
	}
	if len(rep.Deferred) == 0 {
		t.Fatalf("expected deferred records in report")
	}
	t.Logf("容量暂缓: %s", rep.Reason)
	for _, d := range rep.Deferred {
		t.Logf("  deferred: %s", d)
	}
	// 旧布局未变、读取正常、新后端上没有半拉子副本被提交视图引用。
	if got := s.DescribeLayout("o"); got != layoutBefore {
		t.Fatalf("layout must stay unchanged when target has no capacity")
	}
	got, rerr := s.ReadObject("o")
	if rerr != nil || !bytes.Equal(got, data) {
		t.Fatalf("read after deferred rebalance: %v", rerr)
	}
	// 放宽预算后重试可收敛。
	nb.SetCapacity(0)
	rep2, err := s.Rebalance(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("放宽预算后: %s", rep2.Reason)
	assertConvergedLayout(t, s)
}
