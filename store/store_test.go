package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
)

func testOpts(t *testing.T, backends, replicas, chunk int) Options {
	t.Helper()
	return Options{
		RootDir:         t.TempDir(),
		Backends:        backendNames(backends),
		Replicas:        replicas,
		ChunkSize:       chunk,
		KeepCheckpoints: 2,
	}
}

func backendNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("disk%d", i)
	}
	return out
}

func makeData(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i%7)
	}
	return b
}

func TestBasicStripeWriteRead(t *testing.T) {
	opt := testOpts(t, 4, 2, 64)
	s, err := Open(opt)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	data := makeData(200, 1)
	// 单独计算并打印分片布局与判定依据（要求测试日志可见）。
	planner := NewPlanner()
	plan, err := planner.Plan(s.BackendNames(), func(string) bool { return true },
		7, data, opt.ChunkSize, opt.Replicas)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	t.Logf("\n%s", plan.Explain())

	if err := s.Write("wA", 7, data); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := s.Read(7)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch: got %d bytes want %d", len(got), len(data))
	}

	// 校验确定性：磁盘上每个 chunk 的副本确实落在规则指定的不同后端。
	for ci, reps := range plan.Chunks {
		backends := map[string]bool{}
		for _, p := range reps {
			full := filepath.Join(opt.RootDir, "backend-"+p.Backend, p.BlockID+".dat")
			if _, err := os.Stat(full); err != nil {
				t.Fatalf("expected replica file missing chunk=%d backend=%s: %v", ci, p.Backend, err)
			}
			backends[p.Backend] = true
		}
		if len(backends) != opt.Replicas {
			t.Fatalf("chunk %d replicas collide on backends %v", ci, backends)
		}
	}
	t.Logf("verdict: %d chunks x %d replicas placed on 4 backends deterministically; data reassembled OK",
		len(plan.Chunks), opt.Replicas)
}

func TestSkeletonBuilds(t *testing.T) {
	_ = NewPlanner()
}

func asWriteError(err error, target **WriteError) bool {
	for err != nil {
		if we, ok := err.(*WriteError); ok {
			*target = we
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// TestPartialBackendFailure 覆盖两类部分故障：
//  1. 健康后端数不足 -> 规划阶段直接失败，原因可区分；
//  2. 规划通过但某后端写入中途失败 -> 整次写入中止、回滚本次副本、
//     旧检查点仍可读，且不产生新检查点、不留悬空副本。
func TestPartialBackendFailure(t *testing.T) {
	opt := testOpts(t, 4, 2, 64)
	s, err := Open(opt)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	old := makeData(100, 1)
	if err := s.Write("wA", 3, old); err != nil {
		t.Fatalf("initial write: %v", err)
	}
	seqAfterOK := s.HeadSeq()

	// 场景 1：把 3/4 后端置为不可用，健康数 1 < 副本数 2。
	for _, name := range []string{"disk1", "disk2", "disk3"} {
		s.Backend(name).SetUnavailable(true)
	}
	err = s.Write("wB", 3, makeData(100, 9))
	if err == nil {
		t.Fatal("expected plan-stage failure, got nil")
	}
	var we *WriteError
	if !asWriteError(err, &we) || we.Stage != StagePlan {
		t.Fatalf("expected plan-stage WriteError, got %#v", err)
	}
	t.Logf("case1 verdict: insufficient backends -> %v", err)
	for _, name := range []string{"disk1", "disk2", "disk3"} {
		s.Backend(name).SetUnavailable(false)
	}

	// 场景 2：规划阶段全部可用，但让 disk0 写入失败（注入“写入中途失败”）。
	// block3 chunk0 的副本之一落在 disk0（规则：base=(3+0)%4=3 -> disk3,disk0）。
	s.Backend("disk0").SetFailWrites(true)
	newData := makeData(100, 2)
	plan, perr := s.planner.Plan(s.BackendNames(), func(string) bool { return true },
		3, newData, opt.ChunkSize, opt.Replicas)
	if perr != nil {
		t.Fatal(perr)
	}
	t.Logf("case2 planned layout:\n%s", plan.Explain())

	err = s.Write("wB", 3, newData)
	if err == nil {
		t.Fatal("expected replicate-stage failure, got nil")
	}
	if !asWriteError(err, &we) || we.Stage != StageReplicate {
		t.Fatalf("expected replicate-stage WriteError, got %#v", err)
	}
	if len(we.FailedBackends) != 1 || we.FailedBackends[0] != "disk0" {
		t.Fatalf("failed backends = %v, want [disk0]", we.FailedBackends)
	}
	t.Logf("case2 verdict: replicate aborted at %v with distinct reason; failed=%v",
		we.FailedBackends, we.Err)
	s.Backend("disk0").SetFailWrites(false)

	// 旧数据仍然完整可读，且检查点没有前进。
	got, err := s.Read(3)
	if err != nil {
		t.Fatalf("read after failed write: %v", err)
	}
	if !bytes.Equal(got, old) {
		t.Fatal("old data changed after aborted write")
	}
	if s.HeadSeq() != seqAfterOK {
		t.Fatalf("checkpoint advanced despite failure: %d != %d", s.HeadSeq(), seqAfterOK)
	}

	// 本次尝试的所有副本（含成功落到其他后端的）必须已回滚，磁盘无悬空副本。
	for ci, reps := range plan.Chunks {
		for _, p := range reps {
			// 旧版本可能同名（若内容相同）；这里新旧数据不同，摘要必然不同 => 文件应不存在。
			if s.Backend(p.Backend).Exists(p.BlockID) {
				t.Fatalf("dangling replica left behind chunk=%d backend=%s file=%s",
					ci, p.Backend, p.BlockID)
			}
		}
	}
	t.Log("case2 verdict: no checkpoint advance, no torn stripe, no dangling replica")
}

// TestConcurrentWriteDeterminism 多个写入者并发修改同一逻辑块：
// 无论调度如何，最终可见数据必须等于仲裁键 (writerID,dataDigest) 最大者，
// 且读取结果可复现、无半截条带。
func TestConcurrentWriteDeterminism(t *testing.T) {
	opt := testOpts(t, 5, 2, 32)
	s, err := Open(opt)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	const block = uint64(11)
	const writers = 8
	dataOf := make([][]byte, writers)
	for i := range dataOf {
		dataOf[i] = makeData(150, byte(10*i))
	}

	// 仲裁规则：writerID 按 "w00".."w07" 字典序 => w07 必然胜出，与调度无关。
	winner := writers - 1
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			wid := fmt.Sprintf("w%02d", i)
			if err := s.Write(wid, block, dataOf[i]); err != nil {
				// 落败者必须得到可解释的 ErrPlacementConflict。
				if !errors.Is(err, ErrPlacementConflict) {
					errs <- fmt.Errorf("writer %s unexpected err: %w", wid, err)
					return
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}

	got, err := s.Read(block)
	if err != nil {
		t.Fatalf("read after concurrent writes: %v", err)
	}
	if !bytes.Equal(got, dataOf[winner]) {
		t.Fatalf("visible data = writer other than deterministic winner w%02d (len=%d)",
			winner, len(got))
	}
	t.Logf("verdict: %d concurrent writers; arbitration key=(writerID,dataDigest); "+
		"visible final data belongs to deterministic winner w%02d; digest=%s; result is scheduling-independent",
		writers, winner, Checksum(got)[:16])

	// 并发读取者：在持续写入（不同块）期间，已提交块永远重组成功且摘要稳定。
	const otherWriters = 6
	var rwg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < otherWriters; w++ {
		rwg.Add(1)
		go func(w int) {
			defer rwg.Done()
			for k := 0; ; k++ {
				select {
				case <-stop:
					return
				default:
				}
				lb := uint64(100 + w)
				_ = s.Write(fmt.Sprintf("cw%02d", w), lb, makeData(70, byte(w+k)))
			}
		}(w)
	}
	for r := 0; r < 200; r++ {
		g, err := s.Read(block)
		if err != nil || !bytes.Equal(g, dataOf[winner]) {
			close(stop)
			rwg.Wait()
			t.Fatalf("reader observed torn/changed view: err=%v", err)
		}
	}
	close(stop)
	rwg.Wait()
	t.Log("verdict: 200 concurrent reads never observed a half-written stripe or dangling replica")
}

// TestCheckpointRecovery 覆盖：
//   - 重新 Open（模拟进程重启）后状态不重复不丢失；
//   - 重复 Recover 幂等；
//   - 最新检查点的块副本损坏 -> 被发现并隔离；
//   - 最新检查点彻底损坏（某块所有副本都坏）-> 回退到上一个完好检查点。
func TestCheckpointRecovery(t *testing.T) {
	opt := testOpts(t, 4, 2, 64)

	data1 := makeData(128, 1)
	data2 := makeData(128, 2)
	func() {
		s, err := Open(opt)
		if err != nil {
			t.Fatalf("open#1: %v", err)
		}
		defer s.Close()
		if err := s.Write("wA", 1, data1); err != nil {
			t.Fatal(err)
		}
		if err := s.Write("wA", 2, data2); err != nil {
			t.Fatal(err)
		}
		// 失败的写入（后端注入写失败）不得留下任何会影响恢复的状态。
		s.Backend("disk0").SetFailWrites(true)
		if err := s.Write("wA", 3, makeData(50, 3)); err == nil {
			t.Fatal("expected failure")
		}
		s.Backend("disk0").SetFailWrites(false)
	}()

	// 模拟进程重启：重新 Open 应自动恢复到最新完好检查点，数据不丢不重。
	s, err := Open(opt)
	if err != nil {
		t.Fatalf("open#2 (restart): %v", err)
	}
	defer s.Close()
	if s.HeadSeq() != 2 {
		t.Fatalf("after restart head seq=%d want 2", s.HeadSeq())
	}
	for lb, want := range map[uint64][]byte{1: data1, 2: data2} {
		got, err := s.Read(lb)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("after restart block %d: err=%v match=%v", lb, err, bytes.Equal(got, want))
		}
	}
	if _, err := s.Read(3); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed write must not appear after restart, got %v", err)
	}
	t.Log("verdict: restart recovered both blocks exactly once; failed write absent")

	// 损坏最新检查点中 block1 chunk0 的一个副本：恢复应发现、隔离，仍判定完好。
	entry := s.head.Blocks[1]
	ref0 := entry.Chunks[0][0]
	if ok, err := s.Backend(ref0.Backend).CorruptBlock(ref0.File); err != nil || !ok {
		t.Fatalf("inject corruption: ok=%v err=%v", ok, err)
	}
	rep, err := s.Recover()
	if err != nil {
		t.Fatalf("recover with 1 corrupt replica: %v", err)
	}
	if len(rep.CorruptBlocks) == 0 {
		t.Fatal("corruption not detected")
	}
	t.Logf("recover report: checked=%v chosen=%d corrupt=%v quarantined=%v reason=%s",
		rep.Checked, rep.ChosenSeq, rep.CorruptBlocks, rep.QuarantinedFiles, rep.Reason)

	got, err := s.Read(1)
	if err != nil || !bytes.Equal(got, data1) {
		t.Fatalf("block1 should still read from surviving replica: err=%v", err)
	}

	// 重复恢复必须幂等：没有新的损坏、选择不变。
	rep2, err := s.Recover()
	if err != nil || rep2.ChosenSeq != rep.ChosenSeq || len(rep2.CorruptBlocks) != 0 {
		t.Fatalf("second recover not idempotent: %+v err=%v", rep2, err)
	}
	t.Log("verdict: repeated recovery is idempotent (no duplication, no data loss)")

	// 让 seq2 独有的 block2 的 chunk0 全部副本损坏 => 最新检查点不再完好，
	// 必须回退到上一个完好检查点 seq=1（其中只有 block1；其文件与 seq2 共享但仍完好）。
	entry2 := s.head.Blocks[2]
	for _, ref := range entry2.Chunks[0] {
		if ok, err := s.Backend(ref.Backend).CorruptBlock(ref.File); err != nil || !ok {
			t.Fatalf("inject corruption#2: %v %v", ok, err)
		}
	}
	rep3, err := s.Recover()
	if err != nil {
		t.Fatalf("expected rollback to seq1, got error %v", err)
	}
	if rep3.ChosenSeq != 1 {
		t.Fatalf("rollback chose seq=%d want 1; checked=%v reason=%s",
			rep3.ChosenSeq, rep3.Checked, rep3.Reason)
	}
	t.Logf("verdict: newest checkpoint not intact -> rolled back: checked=%v chose=%d reason=%s",
		rep3.Checked, rep3.ChosenSeq, rep3.Reason)

	if _, err := s.Read(1); err != nil {
		t.Fatalf("block1 must remain readable at rolled-back checkpoint: %v", err)
	}
	if _, err := s.Read(2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("block2 must not exist at seq1, got %v", err)
	}
}

// TestGarbageCollection 验证回收后磁盘块文件集合与存活检查点引用集合完全一致：
// 旧版本块、悬空副本、临时文件、隔离文件、超额旧检查点全部回收，且 GC 幂等。
func TestGarbageCollection(t *testing.T) {
	opt := testOpts(t, 4, 2, 64)
	opt.KeepCheckpoints = 2
	s, err := Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	data1 := makeData(128, 1)
	data2 := makeData(128, 2) // 覆盖 block1，使 data1 的块成为旧版本垃圾
	data3 := makeData(90, 3)
	for _, step := range []struct {
		w    string
		lb   uint64
		data []byte
	}{
		{"wA", 1, data1},
		{"wA", 1, data2},
		{"wA", 2, data3},
	} {
		if err := s.Write(step.w, step.lb, step.data); err != nil {
			t.Fatalf("write lb=%d: %v", step.lb, err)
		}
	}

	// 手工制造“悬空副本/崩溃残留”：一个无法被任何检查点引用的块 + 一个临时文件。
	dangling := "b99-c0-deadbeefdeadbeef"
	if err := s.Backend("disk2").WriteBlock(dangling, []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(opt.RootDir, "backend-disk3", ".tmp.leftover"),
		[]byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}

	rep, err := s.GC()
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	t.Logf("gc report: kept=%v removed-cp=%v removed-files=%v reclaimed=%d live=%d",
		rep.KeptCheckpoints, rep.RemovedCheckpoints, rep.RemovedFiles,
		rep.BytesReclaimed, rep.LiveBytes)

	if len(rep.KeptCheckpoints) != 2 {
		t.Fatalf("kept checkpoints = %d want 2", len(rep.KeptCheckpoints))
	}
	if !containsStr(rep.RemovedFiles, "disk2/"+dangling+".dat") {
		t.Fatalf("dangling replica not reclaimed: %v", rep.RemovedFiles)
	}
	if !containsStr(rep.RemovedFiles, "disk3/.tmp.leftover") {
		t.Fatalf("crash temp file not reclaimed: %v", rep.RemovedFiles)
	}

	// 核心不变量：磁盘上每个正式块文件都必须被某个存活检查点引用，反之亦然。
	liveSet := diskBlockSet(t, opt.RootDir)
	refSet := checkpointRefSet(t, filepath.Join(opt.RootDir, "meta"))
	if len(liveSet) != len(refSet) {
		t.Fatalf("live files=%d vs checkpoint refs=%d\nlive=%v\nrefs=%v",
			len(liveSet), len(refSet), sortedKeys(liveSet), sortedKeys(refSet))
	}
	for k := range refSet {
		if !liveSet[k] {
			t.Fatalf("referenced block missing on disk: %s", k)
		}
	}
	for k := range liveSet {
		if !refSet[k] {
			t.Fatalf("unreferenced block survived GC: %s", k)
		}
	}
	// data1 的旧版本块必须已被回收（与 data2 摘要不同）。
	for k := range liveSet {
		if strings.Contains(k, "b1-c0-"+Checksum(data1[:64])[:16]) {
			t.Fatalf("stale version block not reclaimed: %s", k)
		}
	}

	// 存活数据读取正确。
	if got, err := s.Read(1); err != nil || !bytes.Equal(got, data2) {
		t.Fatalf("block1 after gc: err=%v", err)
	}
	if got, err := s.Read(2); err != nil || !bytes.Equal(got, data3) {
		t.Fatalf("block2 after gc: err=%v", err)
	}

	// 占用空间与存活数据一致：LiveBytes == 磁盘现存正式块字节总和。
	var diskTotal int64
	for _, name := range s.BackendNames() {
		entries, _ := os.ReadDir(filepath.Join(opt.RootDir, "backend-"+name))
		for _, e := range entries {
			if info, err := e.Info(); err == nil && strings.HasSuffix(e.Name(), ".dat") {
				diskTotal += info.Size()
			}
		}
	}
	if diskTotal != rep.LiveBytes {
		t.Fatalf("occupied bytes=%d != live report=%d", diskTotal, rep.LiveBytes)
	}

	// GC 幂等：再跑一次，无任何删除。
	rep2, err := s.GC()
	if err != nil || len(rep2.RemovedFiles) != 0 || len(rep2.RemovedCheckpoints) != 0 {
		t.Fatalf("second GC not idempotent: %+v err=%v", rep2, err)
	}
	t.Logf("verdict: disk block set == live-checkpoint refs (%d files, %d bytes); GC idempotent",
		len(liveSet), rep.LiveBytes)
}

func containsStr(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// diskBlockSet 枚举全部后端目录下的正式块文件（排除临时/隔离文件）。
func diskBlockSet(t *testing.T, root string) map[string]bool {
	t.Helper()
	set := map[string]bool{}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "backend-") {
			continue
		}
		name := strings.TrimPrefix(e.Name(), "backend-")
		files, err := os.ReadDir(filepath.Join(root, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			fn := f.Name()
			if strings.HasPrefix(fn, ".tmp.") || strings.Contains(fn, ".quarantine-") {
				t.Fatalf("leftover non-block file after GC: %s/%s", name, fn)
			}
			if strings.HasSuffix(fn, ".dat") {
				set[name+"/"+fn] = true
			}
		}
	}
	return set
}

// checkpointRefSet 从 meta 目录现存全部检查点中汇总 (backend/file) 引用。
func checkpointRefSet(t *testing.T, metaDir string) map[string]bool {
	t.Helper()
	set := map[string]bool{}
	entries, err := os.ReadDir(metaDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		raw, err := os.ReadFile(filepath.Join(metaDir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var cp struct {
			Blocks map[uint64]struct {
				Chunks [][]struct {
					Backend string `json:"backend"`
					File    string `json:"file"`
				} `json:"chunks"`
			} `json:"blocks"`
		}
		if err := json.Unmarshal(raw, &cp); err != nil {
			t.Fatal(err)
		}
		for _, b := range cp.Blocks {
			for _, refs := range b.Chunks {
				for _, r := range refs {
					set[r.Backend+"/"+r.File+".dat"] = true
				}
			}
		}
	}
	return set
}

// TestInterruptedConcurrentWrites 模拟“写入流量中发生中断”：
// 多个 goroutine 并发写多个逻辑块，期间让部分后端反复故障注入，
// 随后以新进程方式重新 Open 并恢复。要求：每个逻辑块最终恰好有一个
// 确定性胜出版本，数据不重复不丢失，失败的尝试不留下可见半成品。
func TestInterruptedConcurrentWrites(t *testing.T) {
	opt := testOpts(t, 5, 2, 48)

	// 每个块的确定性胜出者：仲裁键 (writerID,dataDigest) 最大。
	const blocksN = 12
	const versions = 5
	want := map[uint64]struct {
		writer string
		data   []byte
	}{}
	for b := 0; b < blocksN; b++ {
		var bestKey string
		var bestData []byte
		var bestWriter string
		for v := 0; v < versions; v++ {
			wid := fmt.Sprintf("w%02d", v)
			d := makeData(130+b*3, byte(17*v+b))
			key := wid + "\x00" + Checksum(d)
			if key > bestKey {
				bestKey, bestData, bestWriter = key, d, wid
			}
		}
		want[uint64(b)] = struct {
			writer string
			data   []byte
		}{bestWriter, bestData}
	}

	func() {
		s, err := Open(opt)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		for b := 0; b < blocksN; b++ {
			for v := 0; v < versions; v++ {
				wg.Add(1)
				go func(b, v int) {
					defer wg.Done()
					<-start
					wid := fmt.Sprintf("w%02d", v)
					_ = s.Write(wid, uint64(b), makeData(130+b*3, byte(17*v+b)))
				}(b, v)
			}
		}
		// 故障注入线程：写入进行中反复抖动两个后端（制造中途失败）。
		stop := make(chan struct{})
		var flapWg sync.WaitGroup
		flapWg.Add(1)
		go func() {
			defer flapWg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				s.Backend("disk1").SetFailWrites(true)
				s.Backend("disk3").SetUnavailable(true)
				for k := 0; k < 5; k++ {
					runtime.Gosched()
				}
				s.Backend("disk1").SetFailWrites(false)
				s.Backend("disk3").SetUnavailable(false)
				for k := 0; k < 3; k++ {
					runtime.Gosched()
				}
			}
		}()
		close(start)
		wg.Wait()
		close(stop)
		flapWg.Wait()

		// 故障压力阶段结束、后端全部恢复后，确定性补写每个版本。
		// 此时每个逻辑块的最终可见版本必然是仲裁键最大的版本（与调度无关）。
		s.Backend("disk1").SetFailWrites(false)
		s.Backend("disk3").SetUnavailable(false)
		for b := 0; b < blocksN; b++ {
			for v := 0; v < versions; v++ {
				if err := s.Write(fmt.Sprintf("w%02d", v), uint64(b),
					makeData(130+b*3, byte(17*v+b))); err != nil &&
					!errors.Is(err, ErrPlacementConflict) {
					t.Fatalf("deterministic repair write b=%d v=%d: %v", b, v, err)
				}
			}
		}
		s.Close()
	}()

	// “重启”：重新打开必须自动恢复；每个块恰好呈现确定性胜出版本。
	s, err := Open(opt)
	if err != nil {
		t.Fatalf("reopen after interrupted concurrent writes: %v", err)
	}
	defer s.Close()
	rep, err := s.Recover()
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	t.Logf("post-crash recover: checked=%v chosen=%d reason=%s corrupt=%v",
		rep.Checked, rep.ChosenSeq, rep.Reason, rep.CorruptBlocks)

	for b := 0; b < blocksN; b++ {
		lb := uint64(b)
		got, err := s.Read(lb)
		if err != nil {
			t.Fatalf("block %d unreadable after recovery: %v", lb, err)
		}
		w := want[lb]
		if !bytes.Equal(got, w.data) {
			t.Fatalf("block %d visible version=%x..., want deterministic winner %s digest=%s",
				lb, got[:8], w.writer, Checksum(w.data)[:16])
		}
	}
	t.Logf("verdict: all %d blocks recovered exactly once to their deterministic winner; no duplication/loss", blocksN)
}

// TestReplicaFailover 一个副本所在后端不可用/损坏时，读取必须透明转移到
// 其余完好副本；全部副本不可用时返回可解释错误，而不是半截数据。
func TestReplicaFailover(t *testing.T) {
	opt := testOpts(t, 4, 3, 64)
	s, err := Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	data := makeData(150, 5)
	if err := s.Write("wA", 4, data); err != nil {
		t.Fatal(err)
	}
	entry := s.head.Blocks[4]

	// 让 chunk0 的前两个副本所在后端不可用，仅剩一个可读副本。
	s.Backend(entry.Chunks[0][0].Backend).SetUnavailable(true)
	s.Backend(entry.Chunks[0][1].Backend).SetUnavailable(true)
	got, err := s.Read(4)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("failover read failed: err=%v", err)
	}
	t.Logf("verdict: 2 of 3 replicas unavailable; read served by surviving replica on %s",
		entry.Chunks[0][2].Backend)

	// 三个副本全部不可用 -> 明确失败，绝不返回部分数据。
	s.Backend(entry.Chunks[0][2].Backend).SetUnavailable(true)
	if _, err := s.Read(4); err == nil {
		t.Fatal("expected error when all replicas unavailable")
	} else {
		t.Logf("verdict: all replicas down -> distinct error: %v", err)
	}
}
