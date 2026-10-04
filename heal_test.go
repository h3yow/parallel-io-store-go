package store

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// findLoc 在提交记录中查找指定 (object, shard, replica) 的位置与路径。
func findLoc(t *testing.T, s *Store, objectID string, shard, replica int) (ReplicaLoc, string) {
	t.Helper()
	for _, rec := range s.wal.Records() {
		if rec.ObjectID != objectID {
			continue
		}
		for _, loc := range rec.Locs {
			if loc.Shard == shard && loc.Replica == replica {
				return loc, VersionedShardPath(rec.ObjectID, rec.Block, loc.Shard, loc.Replica, rec.Version, rec.Writer)
			}
		}
	}
	t.Fatalf("no loc for %s shard=%d replica=%d", objectID, shard, replica)
	return ReplicaLoc{}, ""
}

// TestReadSelfHealsCorruptAndMissing 验证读取自愈：首选副本损坏/缺失时
// 照常返回正确内容，并把完好字节补写回固定位置。
func TestReadSelfHealsCorruptAndMissing(t *testing.T) {
	s, _ := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	data := []byte("self-heal-read-path-payload-0123")
	if err := s.WriteObject("h", data, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	layoutBefore := s.DescribeLayout("h")

	// 损坏 shard0/replica0 的字节，删除 shard1/replica0 的文件。
	locC, pathC := findLoc(t, s, "h", 0, 0)
	corruptFile(t, s.Backend(locC.Backend), pathC)
	locM, pathM := findLoc(t, s, "h", 1, 0)
	if err := s.Backend(locM.Backend).Remove(pathM); err != nil {
		t.Fatal(err)
	}
	t.Logf("注入故障: corrupt %s:%s ; missing %s:%s", locC.Backend, pathC, locM.Backend, pathM)

	// 并发读取：结果必须一致且正确（竞态检测下补写并发安全）。
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := s.ReadObject("h")
			if err != nil {
				t.Errorf("read during heal: %v", err)
				return
			}
			if !bytes.Equal(got, data) {
				t.Errorf("read content mismatch during heal")
			}
		}()
	}
	wg.Wait()

	// 自愈事件：两个位置都被补写成功。
	events := s.HealEvents()
	if len(events) == 0 {
		t.Fatalf("expected heal events")
	}
	repaired := map[string]bool{}
	for _, ev := range events {
		t.Logf("自愈事件: shard=%d replica=%d backend=%s trigger=%s repaired=%v err=%s",
			ev.Shard, ev.Replica, ev.Backend, ev.Trigger, ev.Repaired, ev.Err)
		if ev.Repaired {
			repaired[fmt.Sprintf("%d/%d", ev.Shard, ev.Replica)] = true
		}
	}
	if !repaired["0/0"] || !repaired["1/0"] {
		t.Fatalf("expected both replicas repaired, events=%v", events)
	}

	// 布局未变（补写只回固定位置），巡检确认全部恢复健康。
	if got := s.DescribeLayout("h"); got != layoutBefore {
		t.Fatalf("self-heal must not change layout:\nbefore:\n%s\nafter:\n%s", layoutBefore, got)
	}
	rep, err := s.Scrub()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("自愈后巡检: %s", rep.Reason)
	if rep.Corrupt != 0 || rep.Missing != 0 || rep.DegradedShards != 0 {
		t.Fatalf("expected fully healthy after self-heal: %s", rep.Reason)
	}
}

// TestHealFailureDoesNotAffectRead 验证补写失败只记录、不影响读取结果。
func TestHealFailureDoesNotAffectRead(t *testing.T) {
	s, _ := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	data := []byte("heal-failure-tolerated-payload!")
	if err := s.WriteObject("hf", data, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	loc, path := findLoc(t, s, "hf", 0, 0)
	corruptFile(t, s.Backend(loc.Backend), path)
	// 让补写失败：目标后端写操作注入故障（读仍可用）。
	s.Backend(loc.Backend).InjectFault(FaultWriteFail, 100)

	got, err := s.ReadObject("hf")
	if err != nil {
		t.Fatalf("read must succeed despite heal failure: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("content mismatch")
	}
	var sawFailure bool
	for _, ev := range s.HealEvents() {
		t.Logf("自愈事件: shard=%d replica=%d repaired=%v err=%s", ev.Shard, ev.Replica, ev.Repaired, ev.Err)
		if !ev.Repaired && ev.Err != "" {
			sawFailure = true
		}
	}
	if !sawFailure {
		t.Fatalf("expected a recorded heal failure")
	}

	// 故障解除后，巡检把遗留问题修好。
	s.Backend(loc.Backend).ResetFaults()
	rep, err := s.Scrub()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("巡检修复: %s", rep.Reason)
	if rep.Repaired == 0 {
		t.Fatalf("scrub should repair the leftover corrupt replica")
	}
	rep2, _ := s.Scrub()
	if rep2.Corrupt != 0 || rep2.Repaired != 0 {
		t.Fatalf("second scrub must be clean: %s", rep2.Reason)
	}
}

// TestScrubReportAndDegraded 验证巡检统计可解释：好/坏/缺失分类计数、
// 自动修复、无法修复的降级分片被单独报告。
func TestScrubReportAndDegraded(t *testing.T) {
	s, _ := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	objs := map[string][]byte{
		"s1": []byte("scrub-object-one-payload!"),
		"s2": []byte("scrub-object-two-payload!"),
		"s3": []byte("scrub-object-three-payload"),
	}
	for id, data := range objs {
		if err := s.WriteObject(id, data, WriteOptions{Version: 1, Writer: "w"}); err != nil {
			t.Fatal(err)
		}
	}
	// s1: 损坏一个副本；s2: 删除一个副本；s3: shard0 全部副本损坏（降级）。
	loc1, p1 := findLoc(t, s, "s1", 0, 0)
	corruptFile(t, s.Backend(loc1.Backend), p1)
	loc2, p2 := findLoc(t, s, "s2", 1, 0)
	if err := s.Backend(loc2.Backend).Remove(p2); err != nil {
		t.Fatal(err)
	}
	for r := 0; r < 2; r++ {
		loc3, p3 := findLoc(t, s, "s3", 0, r)
		corruptFile(t, s.Backend(loc3.Backend), p3)
	}
	t.Logf("注入: s1 损坏 1 副本; s2 缺失 1 副本; s3 shard0 全部副本损坏")

	rep, err := s.Scrub()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("巡检报告: %s", rep.Reason)
	for _, h := range rep.Replicas {
		if h.Status != ReplicaOK {
			t.Logf("  %s block=%d shard=%d r%d backend=%s status=%s repaired=%v err=%s",
				h.ObjectID, h.Block, h.Shard, h.Replica, h.Backend, h.Status, h.Repaired, h.RepairErr)
		}
	}
	if rep.Corrupt == 0 || rep.Missing == 0 {
		t.Fatalf("expected corrupt and missing counts, got %s", rep.Reason)
	}
	if rep.Repaired == 0 {
		t.Fatalf("expected repairs, got %s", rep.Reason)
	}
	if rep.DegradedShards == 0 {
		t.Fatalf("expected degraded shard for s3, got %s", rep.Reason)
	}
	// s1/s2 读回正确内容；s3 降级不可读。
	for _, id := range []string{"s1", "s2"} {
		got, rerr := s.ReadObject(id)
		if rerr != nil || !bytes.Equal(got, objs[id]) {
			t.Fatalf("%s must read correctly after scrub: %v", id, rerr)
		}
	}
	if _, rerr := s.ReadObject("s3"); !errors.Is(rerr, ErrBlockDegraded) {
		t.Fatalf("s3 must be degraded: %v", rerr)
	}
	// 第二轮巡检：可修的已修好（无新增修复、无缺失），
	// 降级分片的坏副本仍在（无完好源可修），降级计数稳定。
	rep2, err := s.Scrub()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("第二轮巡检: %s", rep2.Reason)
	if rep2.Repaired != 0 || rep2.Missing != 0 {
		t.Fatalf("second scrub must have nothing left to repair: %s", rep2.Reason)
	}
	if rep2.Corrupt != 2 { // s3 shard0 的两个坏副本依旧（不可修复）
		t.Fatalf("only the degraded shard's replicas may remain corrupt: %s", rep2.Reason)
	}
	if rep2.DegradedShards != rep.DegradedShards {
		t.Fatalf("degraded count unstable: %d vs %d", rep.DegradedShards, rep2.DegradedShards)
	}
}

// TestSelfHealRestoresFileBytes 验证补写后的文件字节与提交记录 CRC 一致，
// 且自愈不改变同一时刻的读取结果。
func TestSelfHealRestoresFileBytes(t *testing.T) {
	s, _ := newTestStore(t, 3, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	data := []byte("restore-exact-bytes-payload!!!")
	if err := s.WriteObject("rb", data, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	loc, p := findLoc(t, s, "rb", 2, 0)
	full := filepath.Join(s.Backend(loc.Backend).rootDir(), filepath.FromSlash(p))
	if err := os.Remove(full); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadObject("rb"); err != nil {
		t.Fatal(err)
	}
	// 文件被补写回来，且内容 CRC 与提交记录一致。
	back, err := s.Backend(loc.Backend).Read(p)
	if err != nil {
		t.Fatalf("healed file missing: %v", err)
	}
	// 找到被补写文件所属的那条提交记录（按路径精确匹配），核对其分片 CRC。
	var checked bool
	for _, rec := range s.wal.Records() {
		if rec.ObjectID != "rb" {
			continue
		}
		for _, l := range rec.Locs {
			if l.Shard == 2 && l.Replica == 0 &&
				VersionedShardPath(rec.ObjectID, rec.Block, 2, 0, rec.Version, rec.Writer) == p {
				want := rec.ShardCRC[2]
				if got := crc32.ChecksumIEEE(back); got != want {
					t.Fatalf("healed bytes crc=%08x want %08x", got, want)
				}
				t.Logf("补写文件 %s:%s CRC=%08x 与提交记录一致", loc.Backend, p, want)
				checked = true
			}
		}
	}
	if !checked {
		t.Fatalf("no commit record found for healed replica")
	}
	// 再读：结果与数据完全一致（自愈不改变读取结果）。
	got, err := s.ReadObject("rb")
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read after heal: %v", err)
	}
	if !strings.Contains(s.DescribeLayout("rb"), "backend=") {
		t.Fatalf("layout description broken")
	}
}
