package store

import (
	"errors"
	"strings"
	"testing"
)

// TestReplicaShortage 健康后端数 < R 时必须放置前失败，且原因可区分。
func TestReplicaShortage(t *testing.T) {
	s, backs := newTestStore(t, 3, Config{BlockSize: 16, Shards: 2, Replicas: 3})
	// 让 1 个后端下线：健康=2 < R=3
	backs[0].InjectFault(FaultOffline, 0)
	err := s.WriteObject("o", []byte("shortage-case-data"), WriteOptions{Version: 1, Writer: "w"})
	if err == nil {
		t.Fatal("期望放置失败，但写入成功")
	}
	if !errors.Is(err, ErrReplicaShortage) {
		t.Fatalf("期望 ErrReplicaShortage，实际: %v", err)
	}
	var pe *PlacementError
	if !errors.As(err, &pe) {
		t.Fatalf("期望 *PlacementError 给出判定依据，实际: %v", err)
	}
	t.Logf("副本不足判定: %v", pe)
	// 任何分片都不应已落盘（preflight 在写之前失败）
	for _, b := range backs {
		if names, _ := b.List("shards"); len(names) != 0 {
			t.Fatalf("放置前失败不应产生任何分片文件，但 backend=%s 有: %v", b.ID(), names)
		}
	}
}

// TestMidWriteFailure 写入中途某后端失败：块不得提交，错误可区分，
// 旧版本（若存在）保持可读；故障恢复后同 Key 重试幂等成功。
func TestMidWriteFailure(t *testing.T) {
	s, backs := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	oldData := []byte("v1-aaaaaaaaaaaaaa")
	if err := s.WriteObject("o", oldData, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}

	// v2 写入：让 backend b 的下一次写失败（多块情况下第一块即失败）。
	backs[1].InjectFault(FaultWriteFail, 1)
	newData := []byte("v2-bbbbbbbbbbbbbb")
	err := s.WriteObject("o", newData, WriteOptions{Version: 2, Writer: "w"})
	if err == nil {
		t.Fatal("期望中途失败，实际成功")
	}
	if !errors.Is(err, ErrBlockIncomplete) {
		t.Fatalf("期望 ErrBlockIncomplete，实际: %v", err)
	}
	var swe *ShardWriteError
	if !errors.As(err, &swe) {
		t.Fatalf("期望错误链中含 *ShardWriteError，实际: %v", err)
	}
	t.Logf("中途失败判定（块未提交）: %v", err)

	// 可见数据必须仍是完整的 v1，绝不允许看到半写的 v2 或混合版本。
	got, err := s.ReadObject("o")
	if err != nil {
		t.Fatalf("旧版本应保持可读: %v", err)
	}
	if string(got) != string(oldData) {
		t.Fatalf("失败后可见数据应为旧版本 v1，实际: %q", got)
	}
	t.Logf("写入失败后当前可见布局:\n%s", s.DescribeLayout("o"))

	// 重试 v2（同 version/writer/内容）：幂等成功。
	backs[1].ResetFaults()
	if err := s.WriteObject("o", newData, WriteOptions{Version: 2, Writer: "w"}); err != nil {
		t.Fatalf("幂等重试应成功: %v", err)
	}
	got, err = s.ReadObject("o")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(newData) {
		t.Fatalf("重试后应为 v2，实际: %q", got)
	}
}

// TestWriteConflict 同 version+writer 不同内容必须确定性报冲突。
func TestWriteConflict(t *testing.T) {
	s, _ := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	if err := s.WriteObject("o", []byte("same-key-payload"), WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	err := s.WriteObject("o", []byte("different-payload!"), WriteOptions{Version: 1, Writer: "w"})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("期望 ErrConflict，实际: %v", err)
	}
	t.Logf("冲突判定: %v", err)
}

// TestCorruptReplicaDetected 一个副本静默损坏时读取必须自动使用健康副本，
// 并能发现损坏；所有副本都损坏时报 ErrBlockDegraded。
func TestCorruptReplicaDetected(t *testing.T) {
	s, backs := newTestStore(t, 4, Config{BlockSize: 16, Shards: 2, Replicas: 2})
	data := []byte("corrupt-check!!!")
	if err := s.WriteObject("o", data, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	t.Logf("损坏前布局:\n%s", s.DescribeLayout("o"))

	// 找到 shard 0 的两个副本位置，损坏其中一个文件。
	rec, _ := s.recordFor("o", 0, snapshotKey{1, "w"})
	target := -1
	for _, loc := range rec.Locs {
		if loc.Shard == 0 {
			target = backsIndex(backs, loc.Backend)
			break
		}
	}
	if target < 0 {
		t.Fatal("找不到 shard0 副本")
	}
	p := VersionedShardPath("o", 0, 0, 0, 1, "w")
	// 直接在磁盘上翻转字节模拟静默损坏。
	corruptFile(t, backs[target], p)

	got, err := s.ReadObject("o")
	if err != nil {
		t.Fatalf("单副本损坏应可由另一副本恢复: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("数据不一致: %q", got)
	}
	t.Logf("判定: shard=0 在 backend=%s 上的副本 CRC 不符，已切换到健康副本", backs[target].ID())

	// 损坏 shard0 的所有副本：读取必须报 ErrBlockDegraded，而不是返回错数据。
	for _, loc := range rec.Locs {
		if loc.Shard != 0 {
			continue
		}
		pp := VersionedShardPath("o", 0, 0, loc.Replica, 1, "w")
		corruptFile(t, s.Backend(loc.Backend), pp)
	}
	_, err = s.ReadObject("o")
	if !errors.Is(err, ErrBlockDegraded) {
		t.Fatalf("全副本损坏期望 ErrBlockDegraded，实际: %v", err)
	}
	if !strings.Contains(err.Error(), "crc mismatch") {
		t.Fatalf("错误应说明判定依据 crc mismatch: %v", err)
	}
	t.Logf("全副本损坏判定: %v", err)
}

// TestOfflineBackendAfterCommit 提交后某后端持续下线：
// R=2 时另一副本仍可读；若恰好两个副本所在后端都下线则报降级。
func TestOfflineBackendAfterCommit(t *testing.T) {
	s, backs := newTestStore(t, 4, Config{BlockSize: 16, Shards: 2, Replicas: 2})
	data := []byte("offline-test!!!!")
	if err := s.WriteObject("o", data, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	rec, _ := s.recordFor("o", 0, snapshotKey{1, "w"})
	// 下线 shard0 副本0 所在后端：仍有副本1。
	var shard0Backends []string
	for _, loc := range rec.Locs {
		if loc.Shard == 0 {
			shard0Backends = append(shard0Backends, loc.Backend)
		}
	}
	s.Backend(shard0Backends[0]).InjectFault(FaultOffline, 0)
	got, err := s.ReadObject("o")
	if err != nil {
		t.Fatalf("单后端下线应仍可读: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("数据不一致: %q", got)
	}
	t.Logf("判定: backend=%s 下线，shard0 改从 backend=%s 读取", shard0Backends[0], shard0Backends[1])

	// 两个副本所在后端都下线：必须明确报降级，不得返回部分数据。
	s.Backend(shard0Backends[1]).InjectFault(FaultOffline, 0)
	_, err = s.ReadObject("o")
	if !errors.Is(err, ErrBlockDegraded) {
		t.Fatalf("期望 ErrBlockDegraded，实际: %v", err)
	}
	_ = backs
}
