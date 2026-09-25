package store

import (
	"bytes"
	"path/filepath"
	"testing"
)

// newTestStore 在临时目录中构造 N 个磁盘后端与存储实例。
func newTestStore(t *testing.T, nBackends int, cfg Config) (*Store, []*DiskBackend) {
	t.Helper()
	if cfg.BlockSize == 0 {
		cfg.BlockSize = 16
	}
	if cfg.Shards == 0 {
		cfg.Shards = 4
	}
	if cfg.Replicas == 0 {
		cfg.Replicas = 2
	}
	root := t.TempDir()
	var backs []*DiskBackend
	for i := 0; i < nBackends; i++ {
		id := string(rune('a' + i))
		b, err := NewDiskBackend(id, filepath.Join(root, id))
		if err != nil {
			t.Fatalf("backend %s: %v", id, err)
		}
		backs = append(backs, b)
	}
	s, err := Open(backs, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, backs
}

// TestBasicWriteRead 验证条带化写入后可精确读回，并打印分片布局。
func TestBasicWriteRead(t *testing.T) {
	s, _ := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	data := []byte("hello-parallel-store-0123456789!!") // 34 字节 -> 3 块
	if err := s.WriteObject("obj1", data, WriteOptions{Version: 1, Writer: "w0"}); err != nil {
		t.Fatalf("write: %v", err)
	}
	t.Logf("分片布局与判定依据:\n%s", s.DescribeLayout("obj1"))
	got, err := s.ReadObject("obj1")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("round-trip mismatch: got %q want %q", got, data)
	}
}

// TestDeterministicPlacement 验证相同对象/后端集合下布局逐字节确定。
func TestDeterministicPlacement(t *testing.T) {
	root := t.TempDir()
	mk := func() *Planner {
		var backs []*DiskBackend
		for _, id := range []string{"a", "b", "c", "d"} {
			b, _ := NewDiskBackend(id, filepath.Join(root, id+"-"+func() string { return id }()))
			backs = append(backs, b)
		}
		return NewPlanner(backs, 3, 2)
	}
	p1, p2 := mk(), mk()
	var backs []*DiskBackend
	for _, b := range p1.all {
		backs = append(backs, b)
	}
	l1, err := p1.BlockLayout("X", 7, backs)
	if err != nil {
		t.Fatal(err)
	}
	l2, err := p2.BlockLayout("X", 7, backs)
	if err != nil {
		t.Fatal(err)
	}
	for si := range l1.Shards {
		for ri := range l1.Shards[si] {
			if l1.Shards[si][ri].Backend != l2.Shards[si][ri].Backend {
				t.Fatalf("placement non-deterministic at s=%d r=%d: %s vs %s",
					si, ri, l1.Shards[si][ri].Backend, l2.Shards[si][ri].Backend)
			}
		}
	}
	t.Logf("确定性布局 (obj=X block=7):")
	for si := range l1.Shards {
		for _, pl := range l1.Shards[si] {
			t.Logf("  shard=%d replica=%d -> backend=%s path=%s", pl.Shard, pl.Replica, pl.Backend, pl.Path)
		}
	}
}
