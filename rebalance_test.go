package store

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// writeObjects 写入一批确定性内容的对象并返回 对象ID -> 内容 快照。
func writeObjects(t *testing.T, s *Store, n int, tag string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%s-%d", tag, i)
		data := []byte(fmt.Sprintf("%s-%d-%048d", tag, i, i))
		if err := s.WriteObject(id, data, WriteOptions{Version: 1, Writer: "w"}); err != nil {
			t.Fatalf("write %s: %v", id, err)
		}
		out[id] = data
	}
	return out
}

// verifyObjects 断言所有对象内容、校验和与快照一致。
func verifyObjects(t *testing.T, s *Store, want map[string][]byte) {
	t.Helper()
	for id, data := range want {
		got, err := s.ReadObject(id)
		if err != nil {
			t.Fatalf("read %s: %v", id, err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("object %s content mismatch after operation", id)
		}
	}
}

// addBackend 创建并加入一个新后端。
func addBackend(t *testing.T, s *Store, id string) *DiskBackend {
	t.Helper()
	b, err := NewDiskBackend(id, filepath.Join(t.TempDir(), id))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AddBackend(b); err != nil {
		t.Fatal(err)
	}
	return b
}

// TestAddBackendWithoutRebalanceKeepsLayout 只加后端不做平衡时：
// 已有布局、已有数据、读取结果完全不变（向后兼容）。
func TestAddBackendWithoutRebalanceKeepsLayout(t *testing.T) {
	s, _ := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	objs := writeObjects(t, s, 4, "pre")
	before := map[string]string{}
	for id := range objs {
		before[id] = s.DescribeLayout(id)
	}
	nb := addBackend(t, s, "z")
	t.Logf("已加入新后端 z（未做重平衡）")
	for id := range objs {
		if got := s.DescribeLayout(id); got != before[id] {
			t.Fatalf("只加后端不应改变已有布局 %s:\nbefore:\n%s\nafter:\n%s", id, before[id], got)
		}
	}
	verifyObjects(t, s, objs)
	// 新后端上不应有任何数据文件。
	if names, _ := nb.List("shards"); len(names) != 0 {
		t.Fatalf("未做重平衡时新后端不应承载旧数据: %v", names)
	}
	t.Logf("判定：只加后端不迁移数据，旧布局逐字节不变")
}

// TestRebalanceConvergesAndIdempotent 重平衡后内容/校验和不变、布局收敛、
// 重复执行幂等，旧位置副本由 GC 回收且占用与存活集合一致。
func TestRebalanceConvergesAndIdempotent(t *testing.T) {
	s, _ := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	objs := writeObjects(t, s, 6, "bal")
	addBackend(t, s, "z")

	rep, err := s.Rebalance()
	if err != nil {
		t.Fatalf("rebalance: %v", err)
	}
	t.Logf("重平衡报告: %+v", rep)
	t.Logf("判定依据: %s", rep.Reason)
	if rep.UpdatedBlocks == 0 || rep.CopiedReplicas == 0 {
		t.Fatalf("加入新后端后应有块迁移: %+v", rep)
	}
	verifyObjects(t, s, objs)
	t.Logf("平衡后布局示例:\n%s", s.DescribeLayout("bal-0"))

	// 重复执行：必须完全收敛，不再复制任何副本。
	rep2, err := s.Rebalance()
	if err != nil {
		t.Fatalf("rebalance #2: %v", err)
	}
	t.Logf("第二次重平衡报告: %+v", rep2)
	if rep2.UpdatedBlocks != 0 || rep2.CopiedReplicas != 0 || rep2.PlannedMoves != 0 {
		t.Fatalf("重复重平衡应幂等收敛: %+v", rep2)
	}

	// GC 回收被取代的旧位置副本；占用与存活集合严格一致。
	gc, err := s.RunGC()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("GC 报告: 删除 %d 文件 %d 字节, 保留 %d 文件 %d 字节",
		len(gc.DeletedFiles), gc.DeletedBytes, gc.KeptFiles, gc.KeptBytes)
	if gc.DeletedBytes == 0 {
		t.Fatal("重平衡后应有旧位置副本被 GC 回收")
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

// TestRebalanceConcurrentReadWrite 重平衡与并发读写同时进行：
// 读取不失败、内容不错，新写入正常提交，最终全部一致。
func TestRebalanceConcurrentReadWrite(t *testing.T) {
	s, _ := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	base := writeObjects(t, s, 4, "base")
	addBackend(t, s, "z")

	var wg sync.WaitGroup
	errCh := make(chan error, 64)
	stop := make(chan struct{})
	// 并发读者：整个重平衡期间反复读旧对象，必须永远读到完整正确内容。
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for id, want := range base {
					got, err := s.ReadObject(id)
					if err != nil {
						errCh <- fmt.Errorf("read %s during rebalance: %w", id, err)
						return
					}
					if !bytes.Equal(got, want) {
						errCh <- fmt.Errorf("read %s got wrong content during rebalance", id)
						return
					}
				}
			}
		}()
	}
	// 并发写者：写全新对象（新版本路径，不与迁移冲突）。
	newObjs := map[string][]byte{}
	var wmu sync.Mutex
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 6; i++ {
				id := fmt.Sprintf("new-%d-%d", w, i)
				data := []byte(fmt.Sprintf("new-%d-%d-%032d", w, i, i))
				if err := s.WriteObject(id, data, WriteOptions{Version: 1, Writer: fmt.Sprintf("w%d", w)}); err != nil {
					errCh <- fmt.Errorf("write %s during rebalance: %w", id, err)
					return
				}
				wmu.Lock()
				newObjs[id] = data
				wmu.Unlock()
			}
		}(w)
	}
	rep, err := s.Rebalance()
	if err != nil {
		t.Fatalf("rebalance under concurrency: %v", err)
	}
	t.Logf("并发期间重平衡报告: %+v", rep)
	close(stop)
	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Fatal(e)
	}
	verifyObjects(t, s, base)
	verifyObjects(t, s, newObjs)
	// 并发后再平衡一次：必须收敛（新写入的对象也纳入布局）。
	rep2, err := s.Rebalance()
	if err != nil {
		t.Fatalf("rebalance settle: %v", err)
	}
	rep3, err := s.Rebalance()
	if err != nil {
		t.Fatalf("rebalance final: %v", err)
	}
	t.Logf("收敛轮: %+v -> %+v", rep2, rep3)
	if rep3.UpdatedBlocks != 0 {
		t.Fatalf("重平衡应收敛到固定布局: %+v", rep3)
	}
	verifyObjects(t, s, base)
	verifyObjects(t, s, newObjs)
}

// TestRebalanceInterruptRetryIdempotent 重平衡中断后重试：
// 部分块已切换、部分未切换；重试必须收敛到同一布局，不重不丢；
// 重启（重放含位置取代记录的 WAL）后结论一致。
func TestRebalanceInterruptRetryIdempotent(t *testing.T) {
	root := t.TempDir()
	var backs []*DiskBackend
	for _, id := range []string{"a", "b", "c", "d"} {
		b, err := NewDiskBackend(id, filepath.Join(root, id))
		if err != nil {
			t.Fatal(err)
		}
		backs = append(backs, b)
	}
	cfg := Config{BlockSize: 16, Shards: 4, Replicas: 2}
	s, err := Open(backs, cfg)
	if err != nil {
		t.Fatal(err)
	}
	objs := writeObjects(t, s, 6, "itr")
	nb, err := NewDiskBackend("z", filepath.Join(root, "z"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AddBackend(nb); err != nil {
		t.Fatal(err)
	}

	// 注入"复制中途目标后端写失败"模拟中断：第一次重平衡必然报错。
	nb.InjectFault(FaultWriteFail, 1)
	rep1, err := s.Rebalance()
	if err == nil {
		t.Fatal("注入写故障时重平衡应失败")
	}
	t.Logf("中断的重平衡: err=%v 部分报告=%+v", err, rep1)
	verifyObjects(t, s, objs) // 中断不得影响读取

	// 故障恢复后重试：必须收敛；再重复执行幂等。
	nb.ResetFaults()
	rep2, err := s.Rebalance()
	if err != nil {
		t.Fatalf("重试重平衡: %v", err)
	}
	t.Logf("重试报告: %+v", rep2)
	rep3, err := s.Rebalance()
	if err != nil {
		t.Fatalf("第三次重平衡: %v", err)
	}
	if rep3.UpdatedBlocks != 0 || rep3.CopiedReplicas != 0 {
		t.Fatalf("重试后应完全收敛: %+v", rep3)
	}
	verifyObjects(t, s, objs)
	layoutBefore := map[string]string{}
	for id := range objs {
		layoutBefore[id] = s.DescribeLayout(id)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：WAL 中的位置取代记录重放后布局与内存结论一致。
	backs2 := []*DiskBackend{}
	for _, id := range []string{"a", "b", "c", "d", "z"} {
		b, err := NewDiskBackend(id, filepath.Join(root, id))
		if err != nil {
			t.Fatal(err)
		}
		backs2 = append(backs2, b)
	}
	s2, err := Open(backs2, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	verifyObjects(t, s2, objs)
	for id := range objs {
		if got := s2.DescribeLayout(id); got != layoutBefore[id] {
			t.Fatalf("重启后布局与重平衡结论不一致 %s:\n内存:\n%s\n重放:\n%s", id, layoutBefore[id], got)
		}
	}
	// 重启后再平衡：依然收敛（幂等）。
	rep4, err := s2.Rebalance()
	if err != nil {
		t.Fatal(err)
	}
	if rep4.UpdatedBlocks != 0 {
		t.Fatalf("重启后重平衡应幂等: %+v", rep4)
	}
	// GC 清理中断遗留的半成品复制文件与旧位置副本。
	gc, err := s2.RunGC()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("中断遗留清理: 删除 %d 文件 %d 字节", len(gc.DeletedFiles), gc.DeletedBytes)
	usage, err := s2.SpaceUsage()
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range s2.CapacityReport() {
		if usage[u.Backend] != u.Used {
			t.Fatalf("backend=%s 占用 %d != 存活记账 %d", u.Backend, usage[u.Backend], u.Used)
		}
	}
	verifyObjects(t, s2, objs)
}
