package store

import (
	"bytes"
	"errors"
	"hash/crc32"
	"testing"
)

// firstRec 取对象块 0 的提交记录（测试内部直接读内存视图）。
func firstRec(t *testing.T, s *Store, objectID string) CommitRecord {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	recs := s.committed[objectID][0]
	if len(recs) == 0 {
		t.Fatalf("object %s block 0 无提交记录", objectID)
	}
	return recs[0]
}

// TestReadSelfHealsCorruptReplica 读取发现首选副本损坏时：
// 照常返回正确内容，并把完好字节补写回既定位置（布局不变）。
func TestReadSelfHealsCorruptReplica(t *testing.T) {
	s, _ := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	data := []byte("heal-me-heal-me!!")
	if err := s.WriteObject("o", data, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	rec := firstRec(t, s, "o")
	layoutBefore := s.DescribeLayout("o")

	// 损坏 shard 0 的 replica 0（首选副本）。
	var victim ReplicaLoc
	for _, loc := range rec.Locs {
		if loc.Shard == 0 && loc.Replica == 0 {
			victim = loc
		}
	}
	path := VersionedShardPath("o", 0, victim.Shard, victim.Replica, 1, "w")
	corruptFile(t, s.backends[victim.Backend], path)
	t.Logf("已损坏副本: backend=%s path=%s", victim.Backend, path)

	// 读取必须照常返回正确内容。
	got, err := s.ReadObject("o")
	if err != nil {
		t.Fatalf("存在其它完好副本时读取不应失败: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("读取内容错误: got %q", got)
	}
	// 自愈应已把完好字节补写回既定位置。
	healed, err := s.backends[victim.Backend].Read(path)
	if err != nil {
		t.Fatalf("自愈后副本应存在: %v", err)
	}
	if crc32.ChecksumIEEE(healed) != rec.ShardCRC[victim.Shard] {
		t.Fatal("自愈补写的字节 CRC 不正确")
	}
	// 修复事件留痕。
	var found bool
	for _, ev := range s.RepairEvents() {
		t.Logf("修复事件: %+v", ev)
		if ev.Trigger == "read" && ev.OK && ev.Backend == victim.Backend && ev.Replica == victim.Replica {
			found = true
		}
	}
	if !found {
		t.Fatal("应记录一次 read 触发的成功修复事件")
	}
	// 布局不因自愈改变。
	if got := s.DescribeLayout("o"); got != layoutBefore {
		t.Fatalf("自愈不得改变布局:\nbefore:\n%s\nafter:\n%s", layoutBefore, got)
	}
	t.Logf("判定：读取返回正确内容，副本已在原位置补写，布局未变")
}

// TestReadSelfHealFailureDoesNotAffectRead 补写失败（后端不可用）只记录，
// 不影响本次读取结果。
func TestReadSelfHealFailureDoesNotAffectRead(t *testing.T) {
	s, _ := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	data := []byte("heal-fail-tolerated")
	if err := s.WriteObject("o", data, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	rec := firstRec(t, s, "o")
	var victim ReplicaLoc
	for _, loc := range rec.Locs {
		if loc.Shard == 0 && loc.Replica == 0 {
			victim = loc
		}
	}
	// 让首选副本所在后端整体下线：读取走另一副本，补写必然失败。
	s.backends[victim.Backend].InjectFault(FaultOffline, 0)
	got, err := s.ReadObject("o")
	if err != nil {
		t.Fatalf("单后端下线不应导致读取失败: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("读取内容错误: got %q", got)
	}
	var found bool
	for _, ev := range s.RepairEvents() {
		t.Logf("修复事件: %+v", ev)
		if ev.Trigger == "read" && !ev.OK && ev.Backend == victim.Backend {
			found = true
		}
	}
	if !found {
		t.Fatal("补写失败应被记录（OK=false）")
	}
	t.Logf("判定：补写失败仅记录，读取结果不受影响")
}

// TestScrubReportsAndRepairs 巡检报告每个副本的好/坏/缺失并自动修复，
// 统计可解释，重复巡检幂等。
func TestScrubReportsAndRepairs(t *testing.T) {
	s, _ := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	objs := writeObjects(t, s, 3, "scrub")
	rec := firstRec(t, s, "scrub-0")

	// 制造一个损坏副本 + 一个缺失副本。
	var corruptLoc, missingLoc ReplicaLoc
	for _, loc := range rec.Locs {
		if loc.Shard == 0 && loc.Replica == 0 {
			corruptLoc = loc
		}
		if loc.Shard == 1 && loc.Replica == 1 {
			missingLoc = loc
		}
	}
	corruptPath := VersionedShardPath("scrub-0", 0, corruptLoc.Shard, corruptLoc.Replica, 1, "w")
	missingPath := VersionedShardPath("scrub-0", 0, missingLoc.Shard, missingLoc.Replica, 1, "w")
	corruptFile(t, s.backends[corruptLoc.Backend], corruptPath)
	if err := s.backends[missingLoc.Backend].Remove(missingPath); err != nil {
		t.Fatal(err)
	}
	t.Logf("已制造: 损坏副本 %s:%s, 缺失副本 %s:%s",
		corruptLoc.Backend, corruptPath, missingLoc.Backend, missingPath)

	rep, err := s.Scrub()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("巡检报告: checkedObjects=%d checkedReplicas=%d ok=%d repaired=%d repairFailed=%d degraded=%d",
		rep.CheckedObjects, rep.CheckedReplicas, rep.OK, rep.Repaired, rep.RepairFailed, rep.Degraded)
	t.Logf("判定依据: %s", rep.Reason)
	if rep.CheckedObjects != 3 {
		t.Fatalf("应检查 3 个对象: %+v", rep)
	}
	if rep.Repaired != 2 || rep.RepairFailed != 0 || rep.Degraded != 0 {
		t.Fatalf("应修复 2 个副本且无降级: %+v", rep)
	}
	var sawCorrupt, sawMissing bool
	for _, f := range rep.Findings {
		if f.ObjectID == "scrub-0" && f.Block == 0 {
			if f.Shard == corruptLoc.Shard && f.Replica == corruptLoc.Replica {
				sawCorrupt = f.Status == ReplicaCorrupt && f.Repaired
				t.Logf("损坏副本判定: %+v", f)
			}
			if f.Shard == missingLoc.Shard && f.Replica == missingLoc.Replica {
				sawMissing = f.Status == ReplicaMissing && f.Repaired
				t.Logf("缺失副本判定: %+v", f)
			}
		}
	}
	if !sawCorrupt || !sawMissing {
		t.Fatalf("巡检应分别报告损坏与缺失副本并标记已修复: corrupt=%v missing=%v", sawCorrupt, sawMissing)
	}
	verifyObjects(t, s, objs)

	// 重复巡检：全部完好，幂等。
	rep2, err := s.Scrub()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("第二次巡检: ok=%d repaired=%d degraded=%d", rep2.OK, rep2.Repaired, rep2.Degraded)
	if rep2.Repaired != 0 || rep2.Degraded != 0 || rep2.OK != rep2.CheckedReplicas {
		t.Fatalf("重复巡检应全部完好: %+v", rep2)
	}
}

// TestScrubDegradedWhenNoIntactReplica 同一分片所有副本都损坏时：
// 巡检如实报告降级，不静默；读取报 ErrBlockDegraded。
func TestScrubDegradedWhenNoIntactReplica(t *testing.T) {
	s, _ := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	data := []byte("degraded-shard!!!")
	if err := s.WriteObject("o", data, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	rec := firstRec(t, s, "o")
	// 损坏 shard 2 的全部副本。
	for _, loc := range rec.Locs {
		if loc.Shard == 2 {
			p := VersionedShardPath("o", 0, loc.Shard, loc.Replica, 1, "w")
			corruptFile(t, s.backends[loc.Backend], p)
			t.Logf("已损坏 shard=2 replica=%d backend=%s", loc.Replica, loc.Backend)
		}
	}
	rep, err := s.Scrub()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("巡检报告: repaired=%d repairFailed=%d degraded=%d", rep.Repaired, rep.RepairFailed, rep.Degraded)
	if rep.Degraded == 0 {
		t.Fatal("整分片副本全坏时应报告降级")
	}
	if _, rerr := s.ReadObject("o"); !errors.Is(rerr, ErrBlockDegraded) {
		t.Fatalf("读取应报 ErrBlockDegraded，实际: %v", rerr)
	}
	t.Logf("判定：降级如实上报（degraded=%d），读取确定性地报降级错误", rep.Degraded)
}
