package store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestInterruptedWriteAcrossRestart 模拟写入中途"进程崩溃"：
// v2 的部分块写完后直接丢弃内存态并重新 Open。重启后：
//  1. 不完整的 v2 整体不可见，v1 完整可读（数据不丢）；
//  2. 用相同 (version, writer, payload) 续写幂等成功（不重不丢）；
//  3. GC 回收重启前残留的悬空分片。
func TestInterruptedWriteAcrossRestart(t *testing.T) {
	root := t.TempDir()
	cfg := Config{BlockSize: 8, Shards: 2, Replicas: 2}
	mkBacks := func() []*DiskBackend {
		var bs []*DiskBackend
		for _, id := range []string{"a", "b", "c"} {
			b, err := NewDiskBackend(id, filepath.Join(root, id))
			if err != nil {
				t.Fatal(err)
			}
			bs = append(bs, b)
		}
		return bs
	}
	v1 := []byte("restart-v1-data-0123") // 19 字节 -> 3 块
	s, _ := Open(mkBacks(), cfg)
	if err := s.WriteObject("o", v1, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	// 第二段生命周期：v2 写到一半，让第 2 块失败，随后"崩溃"（直接丢弃 s）。
	v2 := []byte("restart-v2-data-4567")
	s2, _ := Open(mkBacks(), cfg)
	s2.Backend("b").InjectFault(FaultWriteFail, 1)
	werr := s2.WriteObject("o", v2, WriteOptions{Version: 2, Writer: "w"})
	if !errors.Is(werr, ErrBlockIncomplete) {
		t.Fatalf("期望中途失败 ErrBlockIncomplete，实际: %v", werr)
	}
	// 不调用 Close，模拟进程被杀死；磁盘上残留部分 v2 分片。
	t.Logf("崩溃前布局状态:\n%s", s2.DescribeLayout("o"))

	// 重启。
	s3, _ := Open(mkBacks(), cfg)
	defer s3.Close()
	keys, _ := s3.completeSnapshots("o")
	t.Logf("重启后存在的完整快照: %v", keys)
	got, err := s3.ReadObject("o")
	if err != nil {
		t.Fatalf("重启后 v1 必须可读: %v", err)
	}
	if string(got) != string(v1) {
		t.Fatalf("重启后应见 v1，实际 %q", got)
	}
	// 幂等续写 v2：成功且最终数据正确。
	if err := s3.WriteObject("o", v2, WriteOptions{Version: 2, Writer: "w"}); err != nil {
		t.Fatalf("重启后续写失败: %v", err)
	}
	got, _ = s3.ReadObject("o")
	if string(got) != string(v2) {
		t.Fatalf("续写后应见 v2，实际 %q", got)
	}
	// 回收重启前残留的悬空分片。
	rep, err := s3.RunGC()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("重启后 GC: %s", rep.Reason)
	usage, _ := s3.SpaceUsage()
	live := s3.liveShardFiles()
	var want int64
	for id, ps := range live {
		for p := range ps {
			want += s3.Backend(id).SizeBytes(p)
		}
	}
	var have int64
	for _, n := range usage {
		have += n
	}
	if have != want {
		t.Fatalf("GC 后占用 %d != 存活 %d", have, want)
	}
}

// TestWALTornTailIgnored 手工在 WAL 末尾追加撕裂/损坏字节，
// 重放必须忽略坏帧，且不会让未提交块变成可见。
func TestWALTornTailIgnored(t *testing.T) {
	s, backs := newTestStore(t, 3, Config{BlockSize: 8, Shards: 2, Replicas: 2})
	data := []byte("wal-torn-test!!!")
	if err := s.WriteObject("o", data, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	// 直接在 WAL 文件尾部写垃圾（模拟崩溃时只写了一半的提交帧）。
	walFull := filepath.Join(backs[0].rootDir(), filepath.FromSlash(walPath))
	f, err := os.OpenFile(walFull, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("PWAL\x01\x00\x00\x00garbage-tail-bytes")); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	// 用全新后端实例指向同一目录重新打开。
	var reborn []*DiskBackend
	for _, b := range backs {
		nb, err := NewDiskBackend(b.ID(), b.rootDir())
		if err != nil {
			t.Fatal(err)
		}
		reborn = append(reborn, nb)
	}
	s2, err := Open(reborn, Config{BlockSize: 8, Shards: 2, Replicas: 2})
	if err != nil {
		t.Fatalf("损坏 WAL 尾帧不应导致无法打开: %v", err)
	}
	defer s2.Close()
	got, err := s2.ReadObject("o")
	if err != nil {
		t.Fatalf("完好部分必须可读: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("数据不一致: %q", got)
	}
	// 压缩后的日志必须能继续追加新提交，且再次重启后仍可见。
	v2 := []byte("wal-after-compact!")
	if err := s2.WriteObject("o2", v2, WriteOptions{Version: 9, Writer: "w"}); err != nil {
		t.Fatalf("压缩后续写失败: %v", err)
	}
	_ = s2.Close()
	var reborn2 []*DiskBackend
	for _, b := range reborn {
		nb, _ := NewDiskBackend(b.ID(), b.rootDir())
		reborn2 = append(reborn2, nb)
	}
	s3, err := Open(reborn2, Config{BlockSize: 8, Shards: 2, Replicas: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	g2, err := s3.ReadObject("o2")
	if err != nil || string(g2) != string(v2) {
		t.Fatalf("再次重启后压缩后的新提交丢失: %q err=%v", g2, err)
	}
	g1, err := s3.ReadObject("o")
	if err != nil || string(g1) != string(data) {
		t.Fatalf("再次重启后旧数据丢失: %q err=%v", g1, err)
	}
	t.Log("判定: WAL 尾帧不完整/CRC 不符 -> 打开时压缩截断；压缩后新提交可追加，再次重启不丢")
}
