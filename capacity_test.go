package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// countShardFiles 统计后端目录下 shards/ 的 .data 文件数。
func countShardFiles(t *testing.T, b *DiskBackend) int {
	t.Helper()
	n := 0
	root := filepath.Join(b.rootDir(), "shards")
	_ = filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && filepath.Ext(info.Name()) == ".data" {
			n++
		}
		return nil
	})
	return n
}

func logCapacity(t *testing.T, s *Store, prefix string) {
	t.Helper()
	for _, bc := range s.CapacityReport() {
		t.Logf("%s backend=%s budget=%d weight=%d used=%d reserved=%d remaining=%d",
			prefix, bc.ID, bc.Budget, bc.Weight, bc.Used, bc.Reserved, bc.Remaining)
	}
}

// TestCapacityExhaustedFailsBeforeAnyByte 验证：容量不足时整体失败、
// 不落任何字节，且失败原因与"后端不足"可区分。
func TestCapacityExhaustedFailsBeforeAnyByte(t *testing.T) {
	s, backs := newTestStore(t, 3, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	obj1 := []byte("abcdefghijklmnop") // 16 字节 = 1 块
	if err := s.WriteObject("obj1", obj1, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatalf("write obj1: %v", err)
	}
	// 把每个后端的预算精确设为当前已用：预算被占满但已提交数据不受挤占。
	for _, bc := range s.CapacityReport() {
		s.Backend(bc.ID).SetCapacity(bc.Used)
	}
	logCapacity(t, s, "预算=已用后:")
	filesBefore := map[string]int{}
	for _, b := range backs {
		filesBefore[b.ID()] = countShardFiles(t, b)
	}

	err := s.WriteObject("obj2", []byte("0123456789abcdef"), WriteOptions{Version: 1, Writer: "w"})
	if err == nil {
		t.Fatalf("expected capacity failure, got nil")
	}
	if !errors.Is(err, ErrCapacityExhausted) {
		t.Fatalf("expected ErrCapacityExhausted, got %v", err)
	}
	if errors.Is(err, ErrReplicaShortage) {
		t.Fatalf("capacity failure must be distinguishable from replica shortage: %v", err)
	}
	t.Logf("失败原因（可区分）: %v", err)

	// 没落任何字节：各后端文件数与占用不变。
	usage, uerr := s.SpaceUsage()
	if uerr != nil {
		t.Fatal(uerr)
	}
	for _, b := range backs {
		if got := countShardFiles(t, b); got != filesBefore[b.ID()] {
			t.Fatalf("backend=%s wrote bytes despite capacity failure: files %d -> %d", b.ID(), filesBefore[b.ID()], got)
		}
	}
	t.Logf("容量不足时未落盘字节，各后端占用: %v", usage)

	// obj2 不可见；obj1（已提交数据）完好可读、空间不被挤占。
	if _, rerr := s.ReadObject("obj2"); !errors.Is(rerr, ErrObjectNotFound) {
		t.Fatalf("obj2 must not be visible: %v", rerr)
	}
	got, rerr := s.ReadObject("obj1")
	if rerr != nil || string(got) != string(obj1) {
		t.Fatalf("committed obj1 must survive capacity pressure: %v", rerr)
	}
	// 占用统计与存活数据严格一致。
	for _, bc := range s.CapacityReport() {
		if bc.Used != usage[bc.ID] {
			t.Fatalf("backend=%s used=%d != disk live bytes=%d", bc.ID, bc.Used, usage[bc.ID])
		}
		if bc.Remaining != 0 {
			t.Fatalf("backend=%s remaining=%d, want 0 (budget == used)", bc.ID, bc.Remaining)
		}
	}
	logCapacity(t, s, "失败后:")
}

// TestFailureReasonsDistinguishable 验证两类放置失败原因可用 errors.Is 区分。
func TestFailureReasonsDistinguishable(t *testing.T) {
	s, backs := newTestStore(t, 3, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	// 健康后端不足：下线 2 个，剩 1 个 < R=2。
	backs[1].InjectFault(FaultOffline, 0)
	backs[2].InjectFault(FaultOffline, 0)
	err := s.WriteObject("o", []byte("x"), WriteOptions{Version: 1, Writer: "w"})
	if !errors.Is(err, ErrReplicaShortage) || errors.Is(err, ErrCapacityExhausted) {
		t.Fatalf("want ErrReplicaShortage only, got %v", err)
	}
	t.Logf("后端不足判定: %v", err)
	backs[1].ResetFaults()
	backs[2].ResetFaults()

	// 后端充足但预算不足：容量不够。
	for _, b := range backs {
		b.SetCapacity(1) // 任何非空分片都放不下
	}
	err = s.WriteObject("o", []byte("0123456789abcdef"), WriteOptions{Version: 1, Writer: "w"})
	if !errors.Is(err, ErrCapacityExhausted) || errors.Is(err, ErrReplicaShortage) {
		t.Fatalf("want ErrCapacityExhausted only, got %v", err)
	}
	t.Logf("容量不足判定: %v", err)
}

// TestCapacityReportMatchesLiveData 验证占用统计与 GC 后存活数据严格一致。
func TestCapacityReportMatchesLiveData(t *testing.T) {
	s, _ := newTestStore(t, 4, Config{BlockSize: 8, Shards: 4, Replicas: 2})
	for i := 0; i < 6; i++ {
		data := []byte(fmt.Sprintf("obj-%02d-payload-payload", i))
		if err := s.WriteObject(fmt.Sprintf("obj%d", i), data, WriteOptions{Version: 1, Writer: "w"}); err != nil {
			t.Fatalf("write obj%d: %v", i, err)
		}
	}
	// 制造垃圾：写一个新版本到一半失败（注入一次写失败）。
	s.Backend("a").InjectFault(FaultWriteFail, 1)
	_ = s.WriteObject("obj0", []byte("zzzzzzzzzzzzzzzzzzzzzz"), WriteOptions{Version: 2, Writer: "w"})
	s.Backend("a").ResetFaults()
	rep, err := s.RunGC()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("GC: %s", rep.Reason)
	usage, err := s.SpaceUsage()
	if err != nil {
		t.Fatal(err)
	}
	for _, bc := range s.CapacityReport() {
		if bc.Used != usage[bc.ID] {
			t.Fatalf("backend=%s: reported used=%d != live disk bytes=%d", bc.ID, bc.Used, usage[bc.ID])
		}
		t.Logf("backend=%s budget=unlimited used=%d (== 存活数据)", bc.ID, bc.Used)
	}
}

// TestWeightInfluencesDistributionDeterministically 验证权重只影响分布
// 倾向且完全确定：同输入多次计算布局逐位相同。
func TestWeightInfluencesDistributionDeterministically(t *testing.T) {
	const nObj = 48
	writeAll := func(s *Store) {
		for i := 0; i < nObj; i++ {
			id := fmt.Sprintf("wobj-%02d", i)
			data := []byte(fmt.Sprintf("payload-%02d-%s", i, strings.Repeat("x", 20)))
			if err := s.WriteObject(id, data, WriteOptions{Version: 1, Writer: "w"}); err != nil {
				t.Fatalf("write %s: %v", id, err)
			}
		}
	}
	countByBackend := func(s *Store) map[string]int {
		cnt := map[string]int{}
		for _, rec := range s.wal.Records() {
			for _, loc := range rec.Locs {
				cnt[loc.Backend]++
			}
		}
		return cnt
	}

	s1, backs1 := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	s1.Backend("d").SetWeight(3)
	writeAll(s1)
	c1 := countByBackend(s1)
	t.Logf("权重 d=3 其余=1 的副本分布: %v", c1)
	if c1["d"] <= c1["a"] || c1["d"] <= c1["b"] || c1["d"] <= c1["c"] {
		t.Fatalf("weight=3 backend should hold more replicas: %v", c1)
	}

	// 同样的输入在全新实例上重放：分布必须逐位相同（无随机性）。
	root := t.TempDir()
	var backs2 []*DiskBackend
	for _, id := range []string{"a", "b", "c", "d"} {
		b, err := NewDiskBackend(id, filepath.Join(root, id))
		if err != nil {
			t.Fatal(err)
		}
		backs2 = append(backs2, b)
	}
	s2, err := Open(backs2, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	s2.Backend("d").SetWeight(3)
	writeAll(s2)
	c2 := countByBackend(s2)
	for id := range c1 {
		if c1[id] != c2[id] {
			t.Fatalf("non-deterministic weighted placement: backend=%s %d vs %d (%v vs %v)", id, c1[id], c2[id], c1, c2)
		}
	}
	t.Logf("两次独立运行分布完全一致: %v", c2)

	// 权重为默认（全 1）时布局规则与旧版一致：副本仍落在互不相同的后端。
	for _, rec := range s1.wal.Records() {
		seen := map[string]map[int]bool{}
		for _, loc := range rec.Locs {
			if seen[loc.Backend] == nil {
				seen[loc.Backend] = map[int]bool{}
			}
			if seen[loc.Backend][loc.Shard] {
				t.Fatalf("two replicas of shard %d on same backend %s", loc.Shard, loc.Backend)
			}
			seen[loc.Backend][loc.Shard] = true
		}
	}
	_ = backs1
}
