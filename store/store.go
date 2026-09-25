package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Options 配置存储层。
type Options struct {
	RootDir         string   // 根目录（全部数据都在本地磁盘）
	Backends        []string // 后端目录名（每个名字对应一个独立的模拟后端）
	ChunkSize       int      // 条带 chunk 大小（字节）
	Replicas        int      // 每个 chunk 的副本数
	KeepCheckpoints int      // GC 后保留的完好检查点数量（至少 1，默认 2）
}

// Store 本地并行存储层。
type Store struct {
	opt     Options
	root    string
	planner *Planner

	backends     map[string]*DiskBackend
	backendNames []string // 按名字排序的固定顺序
	manifest     *manifestStore

	mu     sync.Mutex
	closed bool
	head   *Checkpoint // 当前可见检查点（不可变；每次提交整体替换）
	maxSeq int         // 磁盘上见过的最大检查点序号

	blockLocksMu sync.Mutex
	blockLocks   map[uint64]*sync.Mutex
}

// Open 打开（或初始化）存储层，并自动恢复到最近一个完好检查点。
// 若磁盘上存在检查点但没有一个完好，返回包装了 ErrNoCheckpoint 的错误。
func Open(opt Options) (*Store, error) {
	if opt.RootDir == "" {
		return nil, fmt.Errorf("%w: empty root dir", ErrInvalidArgument)
	}
	if len(opt.Backends) == 0 {
		return nil, fmt.Errorf("%w: at least one backend required", ErrInvalidArgument)
	}
	if opt.ChunkSize <= 0 {
		opt.ChunkSize = 4096
	}
	if opt.Replicas <= 0 {
		opt.Replicas = 2
	}
	if opt.KeepCheckpoints <= 0 {
		opt.KeepCheckpoints = 2
	}
	if opt.Replicas > len(opt.Backends) {
		return nil, fmt.Errorf("%w: replicas=%d > backends=%d",
			ErrInsufficientBackends, opt.Replicas, len(opt.Backends))
	}

	root, err := filepath.Abs(opt.RootDir)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve root: %v", ErrInvalidArgument, err)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("%w: create root: %v", ErrBackendIO, err)
	}

	names := append([]string(nil), opt.Backends...)
	sort.Strings(names)
	if err := checkUnique(names); err != nil {
		return nil, err
	}
	backends := make(map[string]*DiskBackend, len(names))
	for _, name := range names {
		if name == "" || strings.ContainsAny(name, `/\`) {
			return nil, fmt.Errorf("%w: illegal backend name %q", ErrInvalidArgument, name)
		}
		b, err := NewDiskBackend(name, filepath.Join(root, "backend-"+name))
		if err != nil {
			return nil, err
		}
		backends[name] = b
	}
	ms, err := newManifestStore(filepath.Join(root, "meta"))
	if err != nil {
		return nil, err
	}

	s := &Store{
		opt:          opt,
		root:         root,
		planner:      NewPlanner(),
		backends:     backends,
		backendNames: names,
		manifest:     ms,
		blockLocks:   map[uint64]*sync.Mutex{},
		head:         &Checkpoint{Seq: 0, Blocks: map[uint64]*BlockEntry{}},
	}

	// 启动即恢复：从最新检查点开始逐个验证，落到最近一个全部可验证的检查点。
	rep, err := s.recoverLocked()
	if err != nil {
		return nil, err
	}
	_ = rep
	return s, nil
}

func checkUnique(names []string) error {
	for i := 1; i < len(names); i++ {
		if names[i] == names[i-1] {
			return fmt.Errorf("%w: duplicated backend name %q", ErrInvalidArgument, names[i])
		}
	}
	return nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *Store) lockForBlock(lb uint64) *sync.Mutex {
	s.blockLocksMu.Lock()
	defer s.blockLocksMu.Unlock()
	mu, ok := s.blockLocks[lb]
	if !ok {
		mu = &sync.Mutex{}
		s.blockLocks[lb] = mu
	}
	return mu
}

// arbitrationKey 决定同一逻辑块并发写入的确定性胜出者：
// 按 (writerID, 整块数据 sha256) 的字典序取较大者；
// writerID 与数据都相同则视为幂等重复写入。
// 该规则与 goroutine 调度顺序无关，结果可复现。
func arbitrationKey(writerID, dataChecksum string) string {
	return writerID + "\x00" + dataChecksum
}

// replicaResult 是单个副本并行落盘的结果。
type replicaResult struct {
	chunk   int
	replica int
	backend string
	err     error
}

// Write 以确定性放置规则写入一个逻辑块（条带化 + 多副本，副本并行落盘）。
//
// 失败语义（绝不静默丢块或错放副本）：
//   - 规划阶段可用后端不足 -> ErrInsufficientBackends；
//   - 任一副本放置失败 -> 整次写入中止，本次已上传的副本全部回滚清理，
//     错误中携带失败阶段与失败后端，且原因可区分（不可用 / I/O）；
//   - 仲裁落败 -> ErrPlacementConflict（本次副本同样回滚）；
//   - 任何失败都不会替换当前可见检查点，因此读者永远看不到半成品。
func (s *Store) Write(writerID string, logicalBlock uint64, data []byte) error {
	if writerID == "" {
		return fmt.Errorf("%w: empty writer id", ErrInvalidArgument)
	}
	if len(data) == 0 {
		return fmt.Errorf("%w: empty write is not allowed", ErrInvalidArgument)
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrReadOnly
	}
	s.mu.Unlock()

	// 同一逻辑块串行化：消除半成品交错，也使仲裁与调度顺序无关。
	mu := s.lockForBlock(logicalBlock)
	mu.Lock()
	defer mu.Unlock()

	// ---- 阶段 1：规划（确定性放置 + 可用后端校验）----
	plan, err := s.planner.Plan(s.backendNames,
		func(name string) bool { return s.backends[name].IsAvailable() },
		logicalBlock, data, s.opt.ChunkSize, s.opt.Replicas)
	if err != nil {
		return &WriteError{Stage: StagePlan, LogicalBlock: logicalBlock, Err: err}
	}

	// ---- 阶段 2：并行放置全部副本 ----
	results := make([][]replicaResult, len(plan.Chunks))
	var wg sync.WaitGroup
	for i, reps := range plan.Chunks {
		results[i] = make([]replicaResult, len(reps))
		for j, pl := range reps {
			wg.Add(1)
			go func(i, j int, pl Placement) {
				defer wg.Done()
				b := s.backends[pl.Backend]
				results[i][j] = replicaResult{
					chunk: i, replica: j, backend: pl.Backend,
					err: b.WriteBlock(pl.BlockID, pl.Data),
				}
			}(i, j, pl)
		}
	}
	wg.Wait()

	// 任何一个副本失败都视为整次写入失败：不接受“少一个副本”的提交。
	var failedBackends []string
	var firstErr error
	for i := range results {
		for j := range results[i] {
			r := results[i][j]
			if r.err != nil {
				failedBackends = append(failedBackends, r.backend)
				if firstErr == nil {
					firstErr = r.err
				}
			}
		}
	}
	if firstErr != nil {
		s.rollbackAttempt(plan, results)
		sort.Strings(failedBackends)
		failedBackends = dedupe(failedBackends)
		return &WriteError{
			Stage:          StageReplicate,
			LogicalBlock:   logicalBlock,
			FailedBackends: failedBackends,
			Err:            firstErr,
		}
	}

	// ---- 阶段 3：原子提交（单提交点 + 确定性仲裁）----
	dataSum := Checksum(data)
	newKey := arbitrationKey(writerID, dataSum)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		s.rollbackAttempt(plan, results)
		return ErrReadOnly
	}

	if existing := s.head.Blocks[logicalBlock]; existing != nil {
		exKey := arbitrationKey(existing.Writer, existing.DataChecksum)
		if exKey == newKey {
			// 幂等重复写入：数据已在上一个完好检查点中，直接成功且不产生新检查点。
			return nil
		}
		if exKey > newKey {
			s.rollbackAttemptLocked(plan, results)
			return &WriteError{
				Stage:        StageCommit,
				LogicalBlock: logicalBlock,
				Err: fmt.Errorf("%w: logical-block=%d existing=(writer=%s digest=%s) loses-to new=(writer=%s digest=%s)",
					ErrPlacementConflict, logicalBlock,
					existing.Writer, existing.DataChecksum[:12], writerID, dataSum[:12]),
			}
		}
	}

	blocks := cloneBlocks(s.head.Blocks)
	entry := &BlockEntry{
		LogicalBlock: logicalBlock,
		Writer:       writerID,
		DataChecksum: dataSum,
		ChunkSize:    plan.ChunkSize,
		Length:       plan.Length,
		Chunks:       make([][]ChunkRef, len(plan.Chunks)),
	}
	for i, reps := range plan.Chunks {
		refs := make([]ChunkRef, len(reps))
		for j, pl := range reps {
			refs[j] = ChunkRef{
				Backend: pl.Backend, File: pl.BlockID,
				Checksum: pl.Checksum, Length: len(pl.Data),
			}
		}
		entry.Chunks[i] = refs
	}
	blocks[logicalBlock] = entry

	next := &Checkpoint{
		Seq:       s.maxSeq + 1,
		WriterID:  writerID,
		CreatedAt: nowISO(),
		Blocks:    blocks,
	}
	if err := s.manifest.save(next); err != nil {
		s.rollbackAttemptLocked(plan, results)
		return &WriteError{Stage: StageCommit, LogicalBlock: logicalBlock, Err: err}
	}
	s.head = next
	s.maxSeq = next.Seq
	return nil
}

// rollbackAttempt 在写入失败后删除本次尝试已成功上传的副本，
// 但绝不删除仍被当前检查点引用的同名文件（内容寻址可能与旧版本同名）。
func (s *Store) rollbackAttempt(plan *PlacementPlan, results [][]replicaResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rollbackAttemptLocked(plan, results)
}

func (s *Store) rollbackAttemptLocked(plan *PlacementPlan, results [][]replicaResult) {
	referenced := map[string]map[string]bool{}
	for _, e := range s.head.Blocks {
		for _, refs := range e.Chunks {
			for _, ref := range refs {
				set, ok := referenced[ref.Backend]
				if !ok {
					set = map[string]bool{}
					referenced[ref.Backend] = set
				}
				set[ref.File] = true
			}
		}
	}
	for i := range results {
		for j := range results[i] {
			if results[i][j].err != nil {
				continue
			}
			pl := plan.Chunks[i][j]
			if referenced[pl.Backend] != nil && referenced[pl.Backend][pl.BlockID] {
				// 旧的完好检查点仍引用同名块，必须保留。
				continue
			}
			_ = s.backends[pl.Backend].RemoveBlock(pl.BlockID)
		}
	}
}

// Read 重组一个逻辑块；优先读取完好副本，损坏副本被隔离并记录，
// 所有副本都不可用时返回可解释错误；组装后再次校验整块摘要，
// 任何“半条带/拼接”都会以 ErrChecksumMismatch 暴露。
func (s *Store) Read(logicalBlock uint64) ([]byte, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrReadOnly
	}
	entry, ok := s.head.Blocks[logicalBlock]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: logical-block=%d", ErrNotFound, logicalBlock)
	}

	var out []byte
	var tried []string
	for i, refs := range entry.Chunks {
		var chunk []byte
		for _, ref := range refs {
			b := s.backends[ref.Backend]
			data, err := b.ReadBlock(ref.File)
			if err != nil {
				tried = append(tried, fmt.Sprintf("chunk[%d]@%s: %v", i, ref.Backend, err))
				if errors.Is(err, ErrChecksumMismatch) {
					// 隔离损坏副本，后续恢复与 GC 都不会再把它当作存活数据。
					if _, qerr := b.QuarantineBlock(ref.File); qerr != nil {
						tried = append(tried, fmt.Sprintf("quarantine@%s: %v", ref.Backend, qerr))
					}
				}
				continue
			}
			if Checksum(data) != ref.Checksum {
				tried = append(tried, fmt.Sprintf("chunk[%d]@%s: manifest checksum mismatch", i, ref.Backend))
				continue
			}
			chunk = data
			break
		}
		if chunk == nil {
			return nil, fmt.Errorf("%w: logical-block=%d chunk=%d no readable replica; attempts: %s",
				ErrBackendIO, logicalBlock, i, strings.Join(tried, "; "))
		}
		out = append(out, chunk...)
	}
	out = out[:entry.Length]
	if got := Checksum(out); got != entry.DataChecksum {
		// 组装结果与提交时的整块摘要不符 => 暴露为损坏，而不是返回半截数据。
		return nil, fmt.Errorf("%w: logical-block=%d assembled digest=%s want=%s (torn stripe)",
			ErrChecksumMismatch, logicalBlock, got, entry.DataChecksum)
	}
	return out, nil
}

// RecoveryReport 描述一次恢复的判定依据。
type RecoveryReport struct {
	ChosenSeq        int      // 最终采用的检查点序号（无检查点时为 0）
	Checked          []int    // 依次检查过的检查点序号（新 -> 旧）
	CorruptBlocks    []string // 被发现损坏的副本（backend/file）
	QuarantinedFiles []string // 已隔离的损坏文件
	Reason           string   // 判定说明
	Empty            bool     // 磁盘上没有任何检查点（空存储）
}

// GCReport 描述一次空间回收的结果。
type GCReport struct {
	KeptCheckpoints    []string // 保留的检查点文件
	RemovedCheckpoints []string
	RemovedFiles       []string // 被删除的块/临时文件（backend/path）
	BytesReclaimed     int64
	LiveBytes          int64 // 存活检查点引用的块文件总字节
}

// Recover 重新扫描全部检查点，回退到最近一个全部可验证的检查点。
// 损坏副本会被隔离，检查点中的坏引用会被修复；重复执行结果幂等。
func (s *Store) Recover() (*RecoveryReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recoverLocked()
}

func (s *Store) recoverLocked() (*RecoveryReport, error) {
	paths, err := s.manifest.list()
	if err != nil {
		return nil, err
	}
	report := &RecoveryReport{}
	if len(paths) == 0 {
		report.Reason = "no checkpoint on disk; initialized empty head"
		report.Empty = true
		s.head = &Checkpoint{Seq: 0, Blocks: map[uint64]*BlockEntry{}}
		s.maxSeq = 0
		return report, nil
	}

	var newestOK *Checkpoint
	for i := len(paths) - 1; i >= 0; i-- {
		path := paths[i]
		cp, err := s.manifest.load(path)
		if err != nil {
			report.Checked = append(report.Checked, seqOf(path))
			report.Reason = fmt.Sprintf("checkpoint seq=%d unreadable: %v", seqOf(path), err)
			continue
		}
		report.Checked = append(report.Checked, cp.Seq)

		ok, rebuilt, corrupt, quarantined, reason := s.verifyCheckpoint(cp)
		report.CorruptBlocks = append(report.CorruptBlocks, corrupt...)
		report.QuarantinedFiles = append(report.QuarantinedFiles, quarantined...)
		if !ok {
			if report.Reason == "" {
				report.Reason = fmt.Sprintf("checkpoint seq=%d not intact: %s", cp.Seq, reason)
			}
			continue
		}
		// 完好：若修复过引用（隔离了坏副本），原地重写该检查点使其仍然自洽。
		if len(quarantined) > 0 || refsChanged(cp, rebuilt) {
			cp.Blocks = rebuilt
			if err := s.manifest.rewrite(path, cp); err != nil {
				return nil, err
			}
		} else {
			cp.Blocks = rebuilt
		}
		newestOK = cp
		report.ChosenSeq = cp.Seq
		report.Reason = fmt.Sprintf("chose seq=%d as newest fully-verifiable checkpoint (%s)",
			cp.Seq, reason)
		break
	}

	if newestOK == nil {
		return report, fmt.Errorf("%w: tried %v; %s",
			ErrNoCheckpoint, report.Checked, report.Reason)
	}
	s.head = newestOK
	maxSeq := 0
	for _, p := range paths {
		if v := seqOf(p); v > maxSeq {
			maxSeq = v
		}
	}
	s.maxSeq = maxSeq
	return report, nil
}

// verifyCheckpoint 验证一个检查点：每个 chunk 至少有一个可读取且校验正确的副本。
// 返回的 rebuilt 仅包含“已验证可读”的副本引用；corrupt/quarantined 记录损坏副本。
func (s *Store) verifyCheckpoint(cp *Checkpoint) (ok bool,
	rebuilt map[uint64]*BlockEntry, corrupt, quarantined []string, reason string) {

	rebuilt = map[uint64]*BlockEntry{}
	keys := make([]uint64, 0, len(cp.Blocks))
	for k := range cp.Blocks {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	for _, lb := range keys {
		entry := cp.Blocks[lb]
		ne := &BlockEntry{
			LogicalBlock: entry.LogicalBlock, Writer: entry.Writer,
			DataChecksum: entry.DataChecksum, ChunkSize: entry.ChunkSize,
			Length: entry.Length, Chunks: make([][]ChunkRef, 0, len(entry.Chunks)),
		}
		for ci, refs := range entry.Chunks {
			var good []ChunkRef
			for _, ref := range refs {
				b, exists := s.backends[ref.Backend]
				if !exists {
					corrupt = append(corrupt, fmt.Sprintf("%s/%s (backend gone)", ref.Backend, ref.File))
					continue
				}
				data, err := b.ReadBlock(ref.File)
				if err != nil {
					if errors.Is(err, ErrChecksumMismatch) {
						corrupt = append(corrupt, fmt.Sprintf("%s/%s (%v)", ref.Backend, ref.File, err))
						if q, qerr := b.QuarantineBlock(ref.File); qerr == nil && q != "" {
							quarantined = append(quarantined, ref.Backend+"/"+q)
						}
					} else {
						corrupt = append(corrupt, fmt.Sprintf("%s/%s (unreadable: %v)", ref.Backend, ref.File, err))
					}
					continue
				}
				if Checksum(data) != ref.Checksum {
					corrupt = append(corrupt, fmt.Sprintf("%s/%s (digest mismatch)", ref.Backend, ref.File))
					if q, qerr := b.QuarantineBlock(ref.File); qerr == nil && q != "" {
						quarantined = append(quarantined, ref.Backend+"/"+q)
					}
					continue
				}
				good = append(good, ref)
			}
			if len(good) == 0 {
				return false, rebuilt, corrupt, quarantined,
					fmt.Sprintf("logical-block=%d chunk=%d has no intact replica", lb, ci)
			}
			ne.Chunks = append(ne.Chunks, good)
		}
		rebuilt[lb] = ne
	}
	return true, rebuilt, corrupt, quarantined,
		fmt.Sprintf("all %d blocks verified, %d corrupt replica(s) detected/handled",
			len(keys), len(corrupt))
}

func refsChanged(orig *Checkpoint, rebuilt map[uint64]*BlockEntry) bool {
	if len(orig.Blocks) != len(rebuilt) {
		return true
	}
	for lb, e1 := range orig.Blocks {
		e2, ok := rebuilt[lb]
		if !ok || len(e1.Chunks) != len(e2.Chunks) {
			return true
		}
		for i := range e1.Chunks {
			if len(e1.Chunks[i]) != len(e2.Chunks[i]) {
				return true
			}
		}
	}
	return false
}

// GC 回收空间：
//   - 仅保留最新的 KeepCheckpoints 个“完好”检查点（其余完好与所有损坏检查点删除）；
//   - 删除所有不被存活检查点引用的块文件（旧版本、失败尝试残留、悬空副本）；
//   - 清理所有写入崩溃可能残留的 .tmp 临时文件；
//   - 回收后磁盘上的块文件集合与存活检查点引用集合完全一致。
func (s *Store) GC() (*GCReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	paths, err := s.manifest.list()
	if err != nil {
		return nil, err
	}
	rep := &GCReport{}

	type kept struct {
		path string
		cp   *Checkpoint
	}
	var survivors []kept
	var anyIntact bool

	// 从新到旧挑出完好检查点，保留前 KeepCheckpoints 个。
	for i := len(paths) - 1; i >= 0; i-- {
		path := paths[i]
		cp, err := s.manifest.load(path)
		if err != nil {
			rep.RemovedCheckpoints = append(rep.RemovedCheckpoints, filepath.Base(path))
			if err := s.manifest.remove(path); err != nil {
				return nil, err
			}
			continue
		}
		ok, rebuilt, corrupt, quarantined, _ := s.verifyCheckpoint(cp)
		_ = corrupt
		_ = quarantined
		if !ok {
			rep.RemovedCheckpoints = append(rep.RemovedCheckpoints, filepath.Base(path))
			rep.BytesReclaimed += fileSize(path)
			if err := s.manifest.remove(path); err != nil {
				return nil, err
			}
			continue
		}
		anyIntact = true
		if len(survivors) < s.opt.KeepCheckpoints {
			cp.Blocks = rebuilt
			survivors = append(survivors, kept{path, cp})
		} else {
			rep.RemovedCheckpoints = append(rep.RemovedCheckpoints, filepath.Base(path))
			rep.BytesReclaimed += fileSize(path)
			if err := s.manifest.remove(path); err != nil {
				return nil, err
			}
		}
	}

	if len(paths) > 0 && !anyIntact {
		return rep, fmt.Errorf("%w: refusing GC: no intact checkpoint among %d files",
			ErrNoCheckpoint, len(paths))
	}

	// 存活块集合：(backend, file)。
	live := map[string]map[string]bool{}
	for _, name := range s.backendNames {
		live[name] = map[string]bool{}
	}
	for _, k := range survivors {
		for _, e := range k.cp.Blocks {
			for _, refs := range e.Chunks {
				for _, ref := range refs {
					if set, ok := live[ref.Backend]; ok {
						set[ref.File+blockSuffix] = true
					}
				}
			}
		}
	}

	for _, name := range s.backendNames {
		b := s.backends[name]
		entries, err := os.ReadDir(b.Directory())
		if err != nil {
			return nil, fmt.Errorf("%w: gc list backend=%s: %v", ErrBackendIO, name, err)
		}
		for _, e := range entries {
			fn := e.Name()
			if e.IsDir() {
				continue
			}
			isGarbage := strings.HasPrefix(fn, tmpPrefix) ||
				strings.Contains(fn, ".quarantine-") ||
				(strings.HasSuffix(fn, blockSuffix) && !live[name][fn])
			if !isGarbage {
				if info, err := e.Info(); err == nil {
					rep.LiveBytes += info.Size()
				}
				continue
			}
			full := filepath.Join(b.Directory(), fn)
			size := int64(0)
			if info, err := e.Info(); err == nil {
				size = info.Size()
			}
			if err := os.Remove(full); err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("%w: gc remove %s/%s: %v", ErrBackendIO, name, fn, err)
			}
			rep.RemovedFiles = append(rep.RemovedFiles, name+"/"+fn)
			rep.BytesReclaimed += size
		}
	}

	// 检查点文件字节已在删除前计入回收量。

	sort.Strings(rep.RemovedCheckpoints)
	sort.Strings(rep.RemovedFiles)
	sort.Slice(survivors, func(i, j int) bool { return survivors[i].cp.Seq < survivors[j].cp.Seq })
	for _, k := range survivors {
		rep.KeptCheckpoints = append(rep.KeptCheckpoints, filepath.Base(k.path))
	}

	if len(survivors) > 0 {
		newest := survivors[len(survivors)-1]
		s.head = newest.cp
		maxSeq := 0
		for _, k := range survivors {
			if k.cp.Seq > maxSeq {
				maxSeq = k.cp.Seq
			}
		}
		s.maxSeq = maxSeq
	} else {
		s.head = &Checkpoint{Seq: 0, Blocks: map[uint64]*BlockEntry{}}
		s.maxSeq = 0
	}
	return rep, nil
}

// Backend 返回指定名字的后端句柄（供测试注入故障）。
func (s *Store) Backend(name string) *DiskBackend { return s.backends[name] }

// BackendNames 返回按名字排序的后端列表。
func (s *Store) BackendNames() []string { return append([]string(nil), s.backendNames...) }

// HeadSeq 返回当前可见检查点序号（供测试与诊断）。
func (s *Store) HeadSeq() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.head.Seq
}

func dedupe(in []string) []string {
	if len(in) <= 1 {
		return in
	}
	out := in[:1]
	for _, v := range in[1:] {
		if v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}

func nowISO() string {
	return nowISOString()
}

func nowISOString() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func fileSize(path string) int64 {
	if info, err := os.Stat(path); err == nil {
		return info.Size()
	}
	return 0
}
