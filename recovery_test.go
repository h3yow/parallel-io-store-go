package store

import (
	"errors"
	"path/filepath"
	"testing"
)

// TestCheckpointAndReopen 创建检查点后重新 Open（模拟进程重启），
// 经 WAL 重放数据不丢不重。
func TestCheckpointAndReopen(t *testing.T) {
	root := t.TempDir()
	cfg := Config{BlockSize: 16, Shards: 4, Replicas: 2}
	mkBacks := func() []*DiskBackend {
		var bs []*DiskBackend
		for _, id := range []string{"a", "b", "c", "d"} {
			b, err := NewDiskBackend(id, filepath.Join(root, id))
			if err != nil {
				t.Fatal(err)
			}
			bs = append(bs, b)
		}
		return bs
	}
	s, _ := Open(mkBacks(), cfg)
	data := []byte("checkpoint-data-0123456789-abcd") // 31 字节
	if err := s.WriteObject("o", data, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	seq, err := s.cp.Create()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("已发布检查点 seq=%d", seq)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, _ := Open(mkBacks(), cfg)
	defer s2.Close()
	got, err := s2.ReadObject("o")
	if err != nil {
		t.Fatalf("重开后读取失败: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("重开后数据变化: %q", got)
	}
	// 再次创建检查点并重复恢复：幂等，不重复不丢失。
	seq2, err := s2.cp.Create()
	if err != nil {
		t.Fatal(err)
	}
	res, err := s2.cp.Restore()
	if err != nil {
		t.Fatalf("恢复: %v", err)
	}
	if res.SelectedSeq != seq2 {
		t.Fatalf("应选中最新完好检查点 %d，实际 %d", seq2, res.SelectedSeq)
	}
	res2, err := s2.cp.Restore()
	if err != nil {
		t.Fatalf("重复恢复: %v", err)
	}
	if res2.SelectedSeq != res.SelectedSeq {
		t.Fatalf("重复恢复结论不稳定: %d vs %d", res2.SelectedSeq, res.SelectedSeq)
	}
	got2, _ := s2.ReadObject("o")
	if string(got2) != string(data) {
		t.Fatalf("重复恢复后数据变化: %q", got2)
	}
	t.Logf("恢复过程与判定:\n  %s", res.Reason)
	for _, p := range res.Tried {
		t.Logf("  probe seq=%d healthy=%v reason=%s", p.Seq, p.Healthy, p.Reason)
	}
}

// TestRestoreFallsBackToLastIntact 制造两个检查点：新版本 cp2 引用的分片
// 被静默损坏，恢复必须发现损坏并回退到 cp1（旧完好版本）。
func TestRestoreFallsBackToLastIntact(t *testing.T) {
	root := t.TempDir()
	cfg := Config{BlockSize: 16, Shards: 2, Replicas: 2}
	mkBacks := func() []*DiskBackend {
		var bs []*DiskBackend
		for _, id := range []string{"a", "b", "c", "d"} {
			b, err := NewDiskBackend(id, filepath.Join(root, id))
			if err != nil {
				t.Fatal(err)
			}
			bs = append(bs, b)
		}
		return bs
	}
	v1 := []byte("version-one-data-0123456789!!")
	v2 := []byte("version-two-data-0123456789!!")

	s, _ := Open(mkBacks(), cfg)
	if err := s.WriteObject("o", v1, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	cp1, err := s.cp.Create()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.WriteObject("o", v2, WriteOptions{Version: 2, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	cp2, err := s.cp.Create()
	if err != nil {
		t.Fatal(err)
	}

	// 损坏 v2 块0 shard0 的全部副本，使 cp2 不再完好。
	rec2, _ := s.recordFor("o", 0, snapshotKey{2, "w"})
	for _, loc := range rec2.Locs {
		if loc.Shard != 0 {
			continue
		}
		p := VersionedShardPath("o", 0, 0, loc.Replica, 2, "w")
		corruptFile(t, s.Backend(loc.Backend), p)
	}
	t.Logf("v2 块0 shard0 的全部副本已被静默损坏；cp1=%d cp2=%d", cp1, cp2)

	res, err := s.cp.Restore()
	if err != nil {
		t.Fatalf("应回退到 cp1 而非报错: %v", err)
	}
	if res.SelectedSeq != cp1 {
		t.Fatalf("应回退到 cp1=%d，实际选中 %d", cp1, res.SelectedSeq)
	}
	for _, probe := range res.Tried {
		t.Logf("探测 cp seq=%d healthy=%v badShards=%d reason=%s",
			probe.Seq, probe.Healthy, len(probe.BadShards), probe.Reason)
		for _, bs := range probe.BadShards {
			t.Logf("    损坏判定: obj=%s block=%d shard=%d backend=%s 原因=%s",
				bs.ObjectID, bs.Block, bs.Shard, bs.Backend, bs.Reason)
		}
	}
	got, err := s.ReadObject("o")
	if err != nil {
		t.Fatalf("回退后读取: %v", err)
	}
	if string(got) != string(v1) {
		t.Fatalf("应回退到 v1 数据，实际 %q", got)
	}
	t.Logf("判定: cp2 损坏已发现，回退到 cp1，可见数据为 v1")

	// 回退必须持久化：关闭并重新打开后，WAL 不应再带回损坏的 v2。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	sReopen, err := Open(mkBacks(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer sReopen.Close()
	gotR, err := sReopen.ReadObject("o")
	if err != nil {
		t.Fatalf("回退后重启读取: %v", err)
	}
	if string(gotR) != string(v1) {
		t.Fatalf("回退未持久化，重启后得到 %q（应仍为 v1）", gotR)
	}
	keys, _ := sReopen.completeSnapshots("o")
	for _, k := range keys {
		if k.Version == 2 {
			t.Fatalf("回退后重启不应再出现 v2 快照: %v", keys)
		}
	}
	t.Logf("判定: WAL 已压缩到 cp1，重启后快照=%v，回退持久", keys)
}

// TestAllCheckpointsCorrupt 所有检查点都损坏时必须返回 ErrNoCheckpoint，
// 并给出每个检查点的探测依据。
func TestAllCheckpointsCorrupt(t *testing.T) {
	root := t.TempDir()
	cfg := Config{BlockSize: 16, Shards: 2, Replicas: 2}
	var backs []*DiskBackend
	for _, id := range []string{"a", "b", "c", "d"} {
		b, _ := NewDiskBackend(id, filepath.Join(root, id))
		backs = append(backs, b)
	}
	s, _ := Open(backs, cfg)
	if err := s.WriteObject("o", []byte("only-version!!!!"), WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.cp.Create(); err != nil {
		t.Fatal(err)
	}
	// 损坏块0 shard0 的全部副本。
	rec, _ := s.recordFor("o", 0, snapshotKey{1, "w"})
	for _, loc := range rec.Locs {
		if loc.Shard != 0 {
			continue
		}
		corruptFile(t, s.Backend(loc.Backend), VersionedShardPath("o", 0, 0, loc.Replica, 1, "w"))
	}
	_, err := s.cp.Restore()
	if !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("期望 ErrNoCheckpoint，实际: %v", err)
	}
	t.Logf("无完好检查点判定: %v", err)
}

// TestGCReclaimsGarbageAndMatchesLiveData 写入失败的悬空分片、被取代的旧
// 版本分片都应被 GC 回收；回收后占用空间与存活数据逐字节一致，且 GC 幂等。
func TestGCReclaimsGarbageAndMatchesLiveData(t *testing.T) {
	s, backs := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	v1 := []byte("gc-version-one-0123456789ab")
	v2 := []byte("gc-version-two-0123456789ab")
	if err := s.WriteObject("o", v1, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	// 制造悬空分片：让一次 v2 写入中途失败。
	backs[2].InjectFault(FaultWriteFail, 1)
	_ = s.WriteObject("o", v2, WriteOptions{Version: 2, Writer: "w"})
	backs[2].ResetFaults()
	// 再成功写 v3：v1 成为旧版本垃圾，v2 是悬空垃圾。
	v3 := []byte("gc-version-three-0123456789")
	if err := s.WriteObject("o", v3, WriteOptions{Version: 3, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	before, _ := s.SpaceUsage()
	totalBefore := int64(0)
	for _, n := range before {
		totalBefore += n
	}

	rep, err := s.RunGC()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("GC 判定依据: %s", rep.Reason)
	for _, f := range rep.DeletedFiles {
		t.Logf("  删除垃圾分片: %s", f)
	}
	if len(rep.DeletedFiles) == 0 {
		t.Fatal("期望删除悬空/旧版本分片，实际无删除")
	}

	// 存活数据仍精确可读。
	got, err := s.ReadObject("o")
	if err != nil || string(got) != string(v3) {
		t.Fatalf("GC 后数据异常: %q err=%v", got, err)
	}

	// 占用必须恰好等于存活记录引用的字节数。
	usage, _ := s.SpaceUsage()
	var wantTotal int64
	live := s.liveShardFiles()
	for backID, paths := range live {
		for p := range paths {
			wantTotal += s.Backend(backID).SizeBytes(p)
		}
	}
	var gotTotal int64
	for _, n := range usage {
		gotTotal += n
	}
	if gotTotal != wantTotal {
		t.Fatalf("GC 后占用与存活数据不一致: got=%d want=%d (before=%d)", gotTotal, wantTotal, totalBefore)
	}
	// 再跑一次 GC：幂等，不应再删除任何文件。
	rep2, err := s.RunGC()
	if err != nil {
		t.Fatal(err)
	}
	if len(rep2.DeletedFiles) != 0 {
		t.Fatalf("GC 不幂等，第二次仍删除: %v", rep2.DeletedFiles)
	}
	t.Logf("GC 后占用=%d 字节，与存活集合逐文件一致；重复 GC 删除数=%d", gotTotal, len(rep2.DeletedFiles))
}
