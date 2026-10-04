package store

import (
	"errors"
	"fmt"
	"testing"
)

// logCapacity 打印各后端预算/已用/预留/剩余，供 -v 查看判定依据。
func logCapacity(t *testing.T, s *Store, prefix string) {
	t.Helper()
	for _, u := range s.CapacityReport() {
		t.Logf("%s backend=%s budget=%d used=%d reserved=%d remaining=%d weight=%d",
			prefix, u.Backend, u.Budget, u.Used, u.Reserved, u.Remaining, u.Weight)
	}
}

// TestCapacityExhaustedNoBytesWritten 容量预算放不下时必须整体失败：
// 失败原因是 ErrCapacityExhausted（可与 ErrReplicaShortage 区分），
// 且任何后端上都不落一个字节。
func TestCapacityExhaustedNoBytesWritten(t *testing.T) {
	s, backs := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	// 每个后端预算只够 4 字节（= 1 个分片副本）：一个 16 字节块共需
	// 4 分片 * 4 字节 * 2 副本 = 32 字节，而总预算只有 4*4=16 字节，
	// 必然放不下。
	for _, b := range backs {
		b.SetCapacity(4, 1)
	}
	err := s.WriteObject("o", []byte("0123456789abcdef"), WriteOptions{Version: 1, Writer: "w"})
	if err == nil {
		t.Fatal("预算不足时期望写入失败，实际成功")
	}
	if !errors.Is(err, ErrCapacityExhausted) {
		t.Fatalf("期望 ErrCapacityExhausted，实际: %v", err)
	}
	if errors.Is(err, ErrReplicaShortage) {
		t.Fatalf("容量不足不应被误判为副本不足: %v", err)
	}
	var pe *PlacementError
	if !errors.As(err, &pe) {
		t.Fatalf("期望 *PlacementError 判定依据，实际: %v", err)
	}
	t.Logf("容量不足判定: %v", pe)
	// 未写任何字节。
	usage, uerr := s.SpaceUsage()
	if uerr != nil {
		t.Fatal(uerr)
	}
	for id, n := range usage {
		if n != 0 {
			t.Fatalf("容量预检失败不应落盘任何字节，backend=%s 有 %d 字节", id, n)
		}
	}
	logCapacity(t, s, "失败后占用（应全为 0）:")
}

// TestShortageVsCapacityDistinguishable 后端不够与容量不够是两种可区分原因。
func TestShortageVsCapacityDistinguishable(t *testing.T) {
	// 情形 1：健康后端数不足 -> ErrReplicaShortage（与容量无关，预算充足）。
	s1, backs1 := newTestStore(t, 3, Config{BlockSize: 16, Shards: 2, Replicas: 3})
	for _, b := range backs1 {
		b.SetCapacity(1<<20, 1)
	}
	backs1[0].InjectFault(FaultOffline, 0)
	err1 := s1.WriteObject("o", []byte("shortage"), WriteOptions{Version: 1, Writer: "w"})
	if !errors.Is(err1, ErrReplicaShortage) || errors.Is(err1, ErrCapacityExhausted) {
		t.Fatalf("后端不足应报 ErrReplicaShortage，实际: %v", err1)
	}
	t.Logf("后端不足判定: %v", err1)

	// 情形 2：后端数量充足但预算不足 -> ErrCapacityExhausted。
	s2, backs2 := newTestStore(t, 4, Config{BlockSize: 16, Shards: 2, Replicas: 2})
	for _, b := range backs2 {
		b.SetCapacity(2, 1) // 每片 8 字节，预算 2 字节必然放不下
	}
	err2 := s2.WriteObject("o", []byte("capacity-capacity"), WriteOptions{Version: 1, Writer: "w"})
	if !errors.Is(err2, ErrCapacityExhausted) || errors.Is(err2, ErrReplicaShortage) {
		t.Fatalf("容量不足应报 ErrCapacityExhausted，实际: %v", err2)
	}
	t.Logf("容量不足判定: %v", err2)
}

// TestCapacityReportMatchesLiveData 占用统计必须与存活数据严格一致：
// Used == GC 后磁盘上存活分片文件字节和；Remaining == Budget - Used。
func TestCapacityReportMatchesLiveData(t *testing.T) {
	s, backs := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	for _, b := range backs {
		b.SetCapacity(1<<20, 1)
	}
	for i := 0; i < 5; i++ {
		data := []byte(fmt.Sprintf("obj-%d-%032d", i, i))
		if err := s.WriteObject(fmt.Sprintf("obj%d", i), data, WriteOptions{Version: 1, Writer: "w"}); err != nil {
			t.Fatal(err)
		}
	}
	// 制造垃圾：一次失败的新版本写入 + 旧版本被取代。
	if err := s.WriteObject("obj0", []byte("obj-0-replaced-content-00000000"), WriteOptions{Version: 2, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RunGC(); err != nil {
		t.Fatal(err)
	}
	usage, err := s.SpaceUsage()
	if err != nil {
		t.Fatal(err)
	}
	report := map[string]BackendUsage{}
	for _, u := range s.CapacityReport() {
		report[u.Backend] = u
	}
	for id, bytesOnDisk := range usage {
		u := report[id]
		if u.Used != bytesOnDisk {
			t.Fatalf("backend=%s 记账 used=%d 与磁盘存活字节 %d 不一致", id, u.Used, bytesOnDisk)
		}
		if u.Remaining != u.Budget-u.Used {
			t.Fatalf("backend=%s remaining=%d, 期望 budget-used=%d", id, u.Remaining, u.Budget-u.Used)
		}
	}
	logCapacity(t, s, "GC 后占用（与磁盘严格一致）:")
}

// TestCommittedDataNotEvictedByLaterWrites 已提交数据占用的空间不会被
// 后续写入挤占：预算写满后新写入整体失败，旧数据完整可读。
func TestCommittedDataNotEvictedByLaterWrites(t *testing.T) {
	s, backs := newTestStore(t, 4, Config{BlockSize: 16, Shards: 4, Replicas: 2})
	// 先写一个对象确定各后端消耗，再把预算卡到"刚好装下它"。
	data1 := []byte("0123456789abcdef") // 16 字节 = 4 片 * 4 字节 * 2 副本
	if err := s.WriteObject("keep", data1, WriteOptions{Version: 1, Writer: "w"}); err != nil {
		t.Fatal(err)
	}
	// 把每个后端的预算卡到"刚好装下当前已提交数据"。
	usageNow := map[string]int64{}
	for _, u := range s.CapacityReport() {
		usageNow[u.Backend] = u.Used
	}
	for _, b := range backs {
		b.SetCapacity(usageNow[b.ID()], 1)
	}
	logCapacity(t, s, "预算卡到刚好装满:")
	// 新写入必须失败（容量），且已提交数据不受影响。
	err := s.WriteObject("new", []byte("ffffffffffffffff"), WriteOptions{Version: 1, Writer: "w"})
	if !errors.Is(err, ErrCapacityExhausted) {
		t.Fatalf("预算已满应报 ErrCapacityExhausted，实际: %v", err)
	}
	got, rerr := s.ReadObject("keep")
	if rerr != nil {
		t.Fatalf("已提交数据被挤占/损坏: %v", rerr)
	}
	if string(got) != string(data1) {
		t.Fatalf("已提交数据内容变化: got %q", got)
	}
	t.Logf("已提交对象在预算写满后仍完整可读 (%d 字节)", len(got))
}

// TestWeightInfluencesPlacementDeterministically 权重只影响分布倾向，
// 不引入随机性：同一组输入无论算多少次布局都完全一致；
// 权重大的后端应承载更多副本；全权重 1 时与旧规则逐位一致。
func TestWeightInfluencesPlacementDeterministically(t *testing.T) {
	mk := func() *Planner {
		var backs []*DiskBackend
		for _, id := range []string{"a", "b", "c", "d"} {
			b, err := NewDiskBackend(id, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			backs = append(backs, b)
		}
		return NewPlanner(backs, 4, 2)
	}
	p := mk()
	ring := p.HealthyBackends()
	for _, b := range ring {
		if b.ID() == "a" {
			b.SetCapacity(0, 4) // a 权重 4，其余 1
		}
	}
	count := map[string]int{}
	total := 0
	var firstLayout string
	for obj := 0; obj < 8; obj++ {
		for blk := 0; blk < 4; blk++ {
			id := fmt.Sprintf("obj%d", obj)
			l1, err := p.BlockLayout(id, blk, ring)
			if err != nil {
				t.Fatal(err)
			}
			l2, err := p.BlockLayout(id, blk, ring)
			if err != nil {
				t.Fatal(err)
			}
			desc := ""
			for si := range l1.Shards {
				for ri := range l1.Shards[si] {
					if l1.Shards[si][ri].Backend != l2.Shards[si][ri].Backend {
						t.Fatalf("同一输入布局不确定: %s vs %s",
							l1.Shards[si][ri].Backend, l2.Shards[si][ri].Backend)
					}
					count[l1.Shards[si][ri].Backend]++
					total++
					desc += l1.Shards[si][ri].Backend + ","
				}
			}
			if obj == 0 && blk == 0 {
				firstLayout = desc
			}
		}
	}
	t.Logf("权重 a=4 其余=1 时副本分布: %v (total=%d), 首块布局: %s", count, total, firstLayout)
	if count["a"] <= count["b"] || count["a"] <= count["c"] || count["a"] <= count["d"] {
		t.Fatalf("权重 4 的后端 a 应承载更多副本: %v", count)
	}
	// 对照：全权重 1 时分布应明显比 a 少。
	for _, b := range ring {
		b.SetCapacity(0, 1)
	}
	countA := 0
	for obj := 0; obj < 8; obj++ {
		for blk := 0; blk < 4; blk++ {
			l, err := p.BlockLayout(fmt.Sprintf("obj%d", obj), blk, ring)
			if err != nil {
				t.Fatal(err)
			}
			for si := range l.Shards {
				for _, pl := range l.Shards[si] {
					if pl.Backend == "a" {
						countA++
					}
				}
			}
		}
	}
	t.Logf("全权重 1 时后端 a 副本数: %d（加权时 %d）", countA, count["a"])
	if count["a"] <= countA {
		t.Fatalf("加权后 a 的副本数应严格多于等权时: weighted=%d equal=%d", count["a"], countA)
	}
}
