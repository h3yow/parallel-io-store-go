package store

// 本文件是并行存储层的主入口。
//
// 数据模型：
//   - 对象按 Config.BlockSize 切成逻辑块（最后一块为短块）；
//   - 每块按字节轮转切成 S 个分片（分片 i 持有第 i, i+S, i+2S... 个字节），
//     每个分片有 R 个副本，S*R 个副本并行写入；
//   - 分片位置由 Planner 确定性决定（见 placement.go）；
//   - 块写入 = 所有 S*R 个副本全部 durable 后，在 WAL 追加提交记录；
//     任一副本失败则整块中止（已写文件成为垃圾，后续由 GC 回收），
//     绝不留下"半提交"的可见状态，也绝不静默降级副本数。
//
// 并发一致性：
//   - 每次写入带 (version, writer)，其在单个块上的去重键为
//     (object, block, version, writer)；同键相同内容幂等，
//     同键不同内容在写任何字节前即返回 ErrConflict；
//   - 读取以 (version, writer) 为对象的原子快照：仅块齐全的最高快照可见，
//     与 WAL 追加顺序、goroutine 调度时机无关 —— 并发结果确定；
//   - 只有 WAL 中存在提交记录的块才可见，因此读不到半条带/悬空副本。

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
	"sync"
)

const (
	defaultBlockSize = 4096
	defaultShards    = 4
	defaultReplicas  = 2
	walPath          = "meta/commit.wal"
)

// Config 是存储层配置。
type Config struct {
	Shards          int    // 每块分片数 S（>=1）
	Replicas        int    // 每分片副本数 R（>=1，且 <= 后端数）
	BlockSize       int    // 逻辑块大小（字节）
	MetadataBackend string // 承载 WAL/检查点等元数据的后端 ID
}

func (c *Config) withDefaults() {
	if c.Shards <= 0 {
		c.Shards = defaultShards
	}
	if c.Replicas <= 0 {
		c.Replicas = defaultReplicas
	}
	if c.BlockSize <= 0 {
		c.BlockSize = defaultBlockSize
	}
}

// WriteOptions 描述一次写入的版本与写入者。
type WriteOptions struct {
	Version int64
	Writer  string
}

// Store 是并行存储层实例。
type Store struct {
	mu        sync.Mutex // 保护内存态：已提交块表、在途声明
	opMu      sync.RWMutex
	backends  map[string]*DiskBackend
	planner   *Planner
	wal       *CommitWAL
	cp        *Checkpointer
	cfg       Config
	metaBack  *DiskBackend
	committed map[string]map[int][]CommitRecord // objectID -> blockIndex -> 已提交记录
	// pending 为"已声明、尚未提交"的键 -> 载荷 CRC。
	// 并发同键不同载荷在写任何字节前即被判 ErrConflict，
	// 保证冲突的失败者不会污染胜利者的版本路径。
	pending map[string]uint32
	// reserved 为各后端"已声明、尚未提交"的在途字节预留。
	// 容量判定 = 已提交存活字节 + 在途预留 + 本次需求 <= 预算，
	// 保证并发写入不会合伙超支，也不会出现"写成功但没地方放"。
	reserved map[string]int64
	// healEvents 记录读取路径上的自愈尝试（观测用，不影响读取结果）。
	healEvents []HealEvent
}

// Open 打开或初始化存储层。打开时自动重放 WAL，折叠重复提交。
func Open(backends []*DiskBackend, cfg Config) (*Store, error) {
	cfg.withDefaults()
	if len(backends) == 0 {
		return nil, errors.New("store: at least one backend required")
	}
	if cfg.Shards < 1 || cfg.Replicas < 1 {
		return nil, errors.New("store: shards and replicas must be >= 1")
	}
	if cfg.Replicas > len(backends) {
		return nil, fmt.Errorf("%w: backends=%d replicas=%d", ErrReplicaShortage, len(backends), cfg.Replicas)
	}
	bm := map[string]*DiskBackend{}
	var order []string
	for _, b := range backends {
		if _, dup := bm[b.id]; dup {
			return nil, fmt.Errorf("store: duplicate backend id %q", b.id)
		}
		bm[b.id] = b
		order = append(order, b.id)
	}
	metaID := cfg.MetadataBackend
	if metaID == "" {
		sort.Strings(order)
		metaID = order[0]
	}
	meta, ok := bm[metaID]
	if !ok {
		return nil, fmt.Errorf("store: metadata backend %q not found", metaID)
	}
	planner := NewPlanner(backends, cfg.Shards, cfg.Replicas)
	wal, err := NewCommitWAL(meta, walPath)
	if err != nil {
		return nil, fmt.Errorf("store: open wal: %w", err)
	}
	s := &Store{
		backends:  bm,
		planner:   planner,
		wal:       wal,
		cfg:       cfg,
		metaBack:  meta,
		committed: map[string]map[int][]CommitRecord{},
		pending:   map[string]uint32{},
		reserved:  map[string]int64{},
	}
	for _, rec := range wal.Records() {
		s.indexRecordLocked(rec)
	}
	s.cp = NewCheckpointer(s)
	return s, nil
}

// indexRecordLocked 把一条提交记录加入内存索引（同 Key 去重）。
func (s *Store) indexRecordLocked(rec CommitRecord) {
	blocks := s.committed[rec.ObjectID]
	if blocks == nil {
		blocks = map[int][]CommitRecord{}
		s.committed[rec.ObjectID] = blocks
	}
	for _, existing := range blocks[rec.Block] {
		if existing.Key() == rec.Key() {
			return
		}
	}
	blocks[rec.Block] = append(blocks[rec.Block], rec)
}

// Close 刷盘并关闭。
func (s *Store) Close() error {
	if s.wal != nil {
		return s.wal.Close()
	}
	return nil
}

// splitShards 按字节轮转把块数据切成 S 个分片（确定性）。
func splitShards(data []byte, s int) [][]byte {
	out := make([][]byte, s)
	for i := 0; i < s; i++ {
		n := (len(data) - i + s - 1) / s
		if n < 0 {
			n = 0
		}
		out[i] = make([]byte, 0, n)
	}
	for i, c := range data {
		out[i%s] = append(out[i%s], c)
	}
	return out
}

// mergeShards 是 splitShards 的逆操作，size 为块原始字节数。
func mergeShards(shards [][]byte, size int) []byte {
	out := make([]byte, size)
	for i := 0; i < size; i++ {
		si := i % len(shards)
		k := i / len(shards)
		out[i] = shards[si][k]
	}
	return out
}

// shardByteSizes 返回 size 字节的块切成 S 个分片后每个分片的字节数（确定性）。
func shardByteSizes(size, S int) []int {
	out := make([]int, S)
	for i := 0; i < S; i++ {
		n := (size - i + S - 1) / S
		if n < 0 {
			n = 0
		}
		out[i] = n
	}
	return out
}

// committedUsageLocked 统计当前生效提交视图在各后端上存活的字节数。
// 该值与 GC 存活根集严格同源：已提交数据的空间不会被后续写入挤占。
func (s *Store) committedUsageLocked() map[string]int64 {
	used := map[string]int64{}
	for _, blocks := range s.committed {
		for _, recs := range blocks {
			for _, rec := range recs {
				sizes := shardByteSizes(rec.Size, s.cfg.Shards)
				for _, loc := range rec.Locs {
					if loc.Shard >= 0 && loc.Shard < len(sizes) {
						used[loc.Backend] += int64(sizes[loc.Shard])
					}
				}
			}
		}
	}
	return used
}

// BackendCapacity 是单个后端的容量视图。
type BackendCapacity struct {
	ID        string
	Budget    int64 // <=0 表示不限容量
	Weight    int
	Used      int64 // 已提交存活数据占用（与 GC 存活集合严格一致）
	Reserved  int64 // 在途写入已预留、尚未提交的字节
	Remaining int64 // 预算剩余；不限容量时为 -1
}

// CapacityReport 返回各后端的预算/已用/预留/剩余（按 ID 排序，确定性）。
func (s *Store) CapacityReport() []BackendCapacity {
	s.mu.Lock()
	defer s.mu.Unlock()
	used := s.committedUsageLocked()
	ids := make([]string, 0, len(s.backends))
	for id := range s.backends {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]BackendCapacity, 0, len(ids))
	for _, id := range ids {
		b := s.backends[id]
		budget := b.Capacity()
		rem := int64(-1)
		if budget > 0 {
			rem = budget - used[id] - s.reserved[id]
		}
		out = append(out, BackendCapacity{
			ID: id, Budget: budget, Weight: b.Weight(),
			Used: used[id], Reserved: s.reserved[id], Remaining: rem,
		})
	}
	return out
}

// checkAndReserveLocked 在持有 s.mu 时校验并预留容量：
// 对每个目标后端要求 已提交存活 + 在途预留 + 本次需求 <= 预算。
// 任一后端不满足则整体失败（不写任何字节、不产生预留），
// 返回的错误可用 errors.Is(err, ErrCapacityExhausted) 与后端不足区分。
func (s *Store) checkAndReserveLocked(objectID string, block int, need map[string]int64) error {
	used := s.committedUsageLocked()
	ids := make([]string, 0, len(need))
	for id := range need {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		b := s.backends[id]
		if b == nil {
			continue
		}
		budget := b.Capacity()
		if budget <= 0 {
			continue // 不限容量
		}
		total := used[id] + s.reserved[id] + need[id]
		if total > budget {
			return &PlacementError{
				ObjectID: objectID, Block: block, Shard: -1,
				Want: s.cfg.Replicas, Healthy: len(s.backends),
				Cause: ErrCapacityExhausted,
				Decision: fmt.Sprintf("preflight capacity: backend=%s budget=%d used=%d reserved=%d need=%d -> %d > %d; abort before any shard write",
					id, budget, used[id], s.reserved[id], need[id], total, budget),
			}
		}
	}
	for id, n := range need {
		s.reserved[id] += n
	}
	return nil
}

// unreserve 释放一次容量预留（提交成功或写入中止后调用）。
func (s *Store) unreserve(need map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, n := range need {
		s.reserved[id] -= n
		if s.reserved[id] <= 0 {
			delete(s.reserved, id)
		}
	}
}

// writeBlock 将一个逻辑块条带化、多副本并行写出，全部成功后原子提交。
//
// 整个块写入期间持有 opMu 读锁：多个写入者仍可并发，但 GC / 恢复这类
// 需要"存活集合与磁盘文件严格对应"的操作必须持有写锁，从而保证 GC
// 不会把"刚 durable 但尚未进入存活索引"的副本误删。
func (s *Store) writeBlock(objectID string, block int, data []byte, opts WriteOptions) error {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	// 1) 放置前检查：健康后端不足立即失败，绝不少放副本。
	ring := s.planner.HealthyBackends()
	if err := s.planner.CanPlace(len(ring)); err != nil {
		return &PlacementError{
			ObjectID: objectID, Block: block, Shard: -1,
			Want: s.cfg.Replicas, Healthy: len(ring), Cause: err,
			Decision: "preflight: healthy backend count < required replica factor R; abort before any shard write",
		}
	}
	layout, err := s.planner.BlockLayout(objectID, block, ring)
	if err != nil {
		return err
	}
	// 2) 计算载荷 CRC，并在写任何字节前声明块键：
	//    - 已提交同键同 CRC：幂等成功，直接返回；
	//    - 已提交同键不同 CRC：ErrConflict；
	//    - 在途同键不同 CRC：ErrConflict（冲突方不会互相覆盖版本路径）；
	//    - 在途同键同 CRC：视为并发幂等重试，直接返回，由先声明者完成。
	sum := crc32.ChecksumIEEE(data)
	key := CommitRecord{ObjectID: objectID, Block: block, Version: opts.Version, Writer: opts.Writer}.Key()
	// 本次写入对各后端的容量需求（按确定性布局汇总分片字节数）。
	sizes := shardByteSizes(len(data), s.cfg.Shards)
	need := map[string]int64{}
	for si := range layout.Shards {
		for _, pl := range layout.Shards[si] {
			need[pl.Backend] += int64(sizes[si])
		}
	}
	s.mu.Lock()
	for _, rec := range s.committed[objectID][block] {
		if rec.Key() == key {
			s.mu.Unlock()
			if rec.Checksum == sum {
				return nil
			}
			return fmt.Errorf("%w: object=%s block=%d v=%d writer=%s", ErrConflict, objectID, block, opts.Version, opts.Writer)
		}
	}
	if crc, busy := s.pending[key]; busy {
		s.mu.Unlock()
		if crc == sum {
			return nil // 并发幂等：另一个写入者正在完成同一提交
		}
		return fmt.Errorf("%w: in-flight object=%s block=%d v=%d writer=%s", ErrConflict, objectID, block, opts.Version, opts.Writer)
	}
	// 容量预检：在写任何字节之前，确认所有目标后端都放得下。
	// 与"健康后端不足"（ErrReplicaShortage）可区分地报 ErrCapacityExhausted。
	if err := s.checkAndReserveLocked(objectID, block, need); err != nil {
		s.mu.Unlock()
		return err
	}
	s.pending[key] = sum
	s.mu.Unlock()
	release := func() {
		s.mu.Lock()
		delete(s.pending, key)
		for id, n := range need {
			s.reserved[id] -= n
			if s.reserved[id] <= 0 {
				delete(s.reserved, id)
			}
		}
		s.mu.Unlock()
	}

	// 3) 分片编码与校验和。
	shards := splitShards(data, s.cfg.Shards)
	shardCRC := make([]uint32, len(shards))
	for i, sh := range shards {
		shardCRC[i] = crc32.ChecksumIEEE(sh)
	}
	rec := CommitRecord{
		ObjectID: objectID, Block: block, Version: opts.Version, Writer: opts.Writer,
		Size: len(data), Checksum: sum, ShardCRC: shardCRC,
	}

	// 4) S*R 副本并行写入；任何失败都收集起来，块最终判定为不完整。
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var writeErrs []*ShardWriteError
	for si := range shards {
		for ri := 0; ri < s.cfg.Replicas; ri++ {
			pl := layout.Shards[si][ri]
			wg.Add(1)
			go func(pl Placement, shardIdx int, payload []byte) {
				defer wg.Done()
				path := VersionedShardPath(objectID, block, shardIdx, pl.Replica, opts.Version, opts.Writer)
				back := s.backends[pl.Backend]
				if werr := back.Write(path, payload); werr != nil {
					errMu.Lock()
					writeErrs = append(writeErrs, &ShardWriteError{
						BackendID: pl.Backend, ObjectID: objectID,
						Block: block, Shard: shardIdx, Replica: pl.Replica, Cause: werr,
					})
					errMu.Unlock()
				}
			}(pl, si, shards[si])
		}
	}
	wg.Wait()
	if len(writeErrs) > 0 {
		sort.Slice(writeErrs, func(i, j int) bool {
			if writeErrs[i].Shard != writeErrs[j].Shard {
				return writeErrs[i].Shard < writeErrs[j].Shard
			}
			return writeErrs[i].Replica < writeErrs[j].Replica
		})
		errs := []error{ErrBlockIncomplete}
		for _, e := range writeErrs {
			errs = append(errs, e)
		}
		errs = append(errs, fmt.Errorf("decision: %d/%d replicas durable; block NOT committed, written shards are garbage pending GC",
			s.cfg.Shards*s.cfg.Replicas-len(writeErrs), s.cfg.Shards*s.cfg.Replicas))
		release()
		return errors.Join(errs...)
	}

	// 5) 所有副本 durable：固化放置位置并追加提交记录（原子可见点）。
	rec.Locs = make([]ReplicaLoc, 0, len(layout.Shards)*s.cfg.Replicas)
	for si := range layout.Shards {
		for _, pl := range layout.Shards[si] {
			rec.Locs = append(rec.Locs, ReplicaLoc{Shard: si, Replica: pl.Replica, Backend: pl.Backend})
		}
	}
	sort.Slice(rec.Locs, func(i, j int) bool {
		if rec.Locs[i].Shard != rec.Locs[j].Shard {
			return rec.Locs[i].Shard < rec.Locs[j].Shard
		}
		return rec.Locs[i].Replica < rec.Locs[j].Replica
	})
	if err := s.wal.Append(rec); err != nil {
		// 理论上仅在并发同 Key 不同内容时发生：副本已落盘但块不生效，交由 GC 回收。
		release()
		return errors.Join(ErrBlockIncomplete, err)
	}
	s.mu.Lock()
	delete(s.pending, key)
	// 预留转为已提交占用：committed 视图从现在开始计入这些字节。
	for id, n := range need {
		s.reserved[id] -= n
		if s.reserved[id] <= 0 {
			delete(s.reserved, id)
		}
	}
	s.indexRecordLocked(rec)
	s.mu.Unlock()
	return nil
}

// snapshotKey 标识对象的一个原子写入快照。
type snapshotKey struct {
	Version int64
	Writer  string
}

// less 定义快照的确定性全序：version 升序，writer 字典序兜底。
func (k snapshotKey) less(o snapshotKey) bool {
	if k.Version != o.Version {
		return k.Version < o.Version
	}
	return k.Writer < o.Writer
}

// completeSnapshots 返回对象所有"块齐全"的快照及其块数（按全序排序）。
// 未完成快照（新版本写到一半失败）不返回：它整体不可见，
// 读取继续落到较低版本的完整快照。
func (s *Store) completeSnapshots(objectID string) ([]snapshotKey, map[snapshotKey]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	blocks := s.committed[objectID]
	maxBlock := -1
	for b := range blocks {
		if b > maxBlock {
			maxBlock = b
		}
	}
	if maxBlock < 0 {
		return nil, nil
	}
	present := map[snapshotKey]map[int]bool{}
	for b, recs := range blocks {
		for _, rec := range recs {
			k := snapshotKey{rec.Version, rec.Writer}
			if present[k] == nil {
				present[k] = map[int]bool{}
			}
			present[k][b] = true
		}
	}
	counts := map[snapshotKey]int{}
	var keys []snapshotKey
	for k, bs := range present {
		if len(bs) == maxBlock+1 { // 块 0..maxBlock 全部存在
			keys = append(keys, k)
			counts[k] = len(bs)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].less(keys[j]) })
	return keys, counts
}

// latestSnapshot 返回最高的完整快照。
func (s *Store) latestSnapshot(objectID string) (snapshotKey, bool) {
	keys, _ := s.completeSnapshots(objectID)
	if len(keys) == 0 {
		return snapshotKey{}, false
	}
	return keys[len(keys)-1], true
}

// visibleRecord 返回某块在指定快照下的提交记录。
func (s *Store) recordFor(objectID string, block int, k snapshotKey) (CommitRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.committed[objectID][block] {
		if r.Version == k.Version && r.Writer == k.Writer {
			return r, true
		}
	}
	return CommitRecord{}, false
}

// readBlock 按提交记录中的固定放置读取并重建块；每个分片按副本序号
// 依次尝试健康副本（位置确定），全部副本不可读则块降级。
func (s *Store) readBlock(rec CommitRecord) ([]byte, error) {
	locs := map[int]map[int]string{} // shard -> replica -> backend
	for _, loc := range rec.Locs {
		if locs[loc.Shard] == nil {
			locs[loc.Shard] = map[int]string{}
		}
		locs[loc.Shard][loc.Replica] = loc.Backend
	}
	shards := make([][]byte, s.cfg.Shards)
	var degraded []error
	for si := 0; si < s.cfg.Shards; si++ {
		reps := locs[si]
		var repOrder []int
		for r := range reps {
			repOrder = append(repOrder, r)
		}
		sort.Ints(repOrder)
		var got []byte
		var lastErr error
		type failedLoc struct {
			replica int
			backend string
			reason  string
		}
		var failed []failedLoc
		for _, r := range repOrder {
			backID := reps[r]
			back := s.backends[backID]
			path := VersionedShardPath(rec.ObjectID, rec.Block, si, r, rec.Version, rec.Writer)
			if back == nil {
				lastErr = fmt.Errorf("shard=%d replica=%d backend=%s: backend not configured", si, r, backID)
				failed = append(failed, failedLoc{r, backID, "backend not configured"})
				continue
			}
			data, err := back.Read(path)
			if err != nil {
				lastErr = fmt.Errorf("shard=%d replica=%d backend=%s: %w", si, r, backID, err)
				failed = append(failed, failedLoc{r, backID, err.Error()})
				continue
			}
			if crc32.ChecksumIEEE(data) != rec.ShardCRC[si] {
				lastErr = fmt.Errorf("shard=%d replica=%d backend=%s: shard crc mismatch (silent corruption)", si, r, backID)
				failed = append(failed, failedLoc{r, backID, "shard crc mismatch"})
				continue
			}
			got = data
			break
		}
		if got == nil {
			degraded = append(degraded, fmt.Errorf("%w: object=%s block=%d shard=%d: %v",
				ErrBlockDegraded, rec.ObjectID, rec.Block, si, lastErr))
			continue
		}
		// 读取自愈：分片已由完好副本重建，顺手把完好字节补写回
		// 缺失/损坏的固定位置（同后端同路径，布局不变）。
		// 补写失败只记录事件，绝不影响本次读取结果。
		for _, f := range failed {
			ev := HealEvent{
				ObjectID: rec.ObjectID, Block: rec.Block, Shard: si, Replica: f.replica,
				Backend: f.backend,
				Path:    VersionedShardPath(rec.ObjectID, rec.Block, si, f.replica, rec.Version, rec.Writer),
				Trigger: f.reason,
			}
			if herr := s.repairReplica(rec, si, f.replica, f.backend, got); herr != nil {
				ev.Err = herr.Error()
			} else {
				ev.Repaired = true
			}
			s.recordHeal(ev)
		}
		shards[si] = got
	}
	if len(degraded) > 0 {
		return nil, errors.Join(append([]error{ErrBlockDegraded}, degraded...)...)
	}
	data := mergeShards(shards, rec.Size)
	if crc32.ChecksumIEEE(data) != rec.Checksum {
		return nil, fmt.Errorf("%w: object=%s block=%d: reconstructed block crc mismatch", ErrBlockDegraded, rec.ObjectID, rec.Block)
	}
	return data, nil
}

// WriteObject 整体写入一个对象（按 opts.Version/Writer 标识版本）。
// 块顺序写出；若某块失败，错误聚合返回，已提交块仍可见且版本完整。
func (s *Store) WriteObject(objectID string, data []byte, opts WriteOptions) error {
	if opts.Writer == "" {
		return errors.New("store: Writer must not be empty")
	}
	if objectID == "" {
		return errors.New("store: objectID must not be empty")
	}
	nBlocks := (len(data) + s.cfg.BlockSize - 1) / s.cfg.BlockSize
	if nBlocks == 0 {
		nBlocks = 1 // 允许空对象：一个长度 0 的块
	}
	var failed []error
	for b := 0; b < nBlocks; b++ {
		start := b * s.cfg.BlockSize
		end := start + s.cfg.BlockSize
		if end > len(data) {
			end = len(data)
		}
		if err := s.writeBlock(objectID, b, data[start:end], opts); err != nil {
			failed = append(failed, fmt.Errorf("block %d: %w", b, err))
		}
	}
	if len(failed) > 0 {
		return errors.Join(failed...)
	}
	return nil
}

// WriteRegion 对对象某偏移做定点写入（offset+len(payload) 不得越界）。
// 区域写以新版本整体重写受影响对象，因此同样遵循块原子提交规则。
func (s *Store) WriteRegion(objectID string, offset int, payload []byte, opts WriteOptions) error {
	cur, err := s.ReadObject(objectID)
	if err != nil {
		return err
	}
	if offset < 0 || offset+len(payload) > len(cur) {
		return fmt.Errorf("region out of range: offset=%d len=%d objectSize=%d", offset, len(payload), len(cur))
	}
	buf := append([]byte(nil), cur...)
	copy(buf[offset:offset+len(payload)], payload)
	return s.WriteObject(objectID, buf, opts)
}

// ReadObject 读取对象当前可见内容。
// 可见性规则：以 (version, writer) 为原子快照，只选择"块齐全"的最高快照；
// 新版本写入中途失败时，整个新版本不可见，读取确定性地回退到旧完整版本。
func (s *Store) ReadObject(objectID string) ([]byte, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	k, ok := s.latestSnapshot(objectID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrObjectNotFound, objectID)
	}
	_, counts := s.completeSnapshots(objectID)
	nBlocks := counts[k]
	var out []byte
	for b := 0; b < nBlocks; b++ {
		rec, ok := s.recordFor(objectID, b, k)
		if !ok {
			// 快照完整性已校验，此处仅为防御性分支。
			return nil, fmt.Errorf("%w: object=%s block=%d v=%d/%s", ErrBlockIncomplete, objectID, b, k.Version, k.Writer)
		}
		data, err := s.readBlock(rec)
		if err != nil {
			return nil, err
		}
		out = append(out, data...)
	}
	return out, nil
}

// DescribeLayout 返回某对象当前可见版本的分片布局说明，
// 供测试日志打印与人工判定（规则、位置、校验和均列出）。
func (s *Store) DescribeLayout(objectID string) string {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	k, ok := s.latestSnapshot(objectID)
	if !ok {
		return fmt.Sprintf("object=%s: <no complete snapshot>", objectID)
	}
	_, counts := s.completeSnapshots(objectID)
	nBlocks := counts[k]
	var sb strings.Builder
	fmt.Fprintf(&sb, "object=%s S=%d R=%d blocks=%d blockSize=%d visible=v%d/writer=%s\n",
		objectID, s.cfg.Shards, s.cfg.Replicas, nBlocks, s.cfg.BlockSize, k.Version, k.Writer)
	sb.WriteString("placement rule: shard s replicas at weightedRing[(FNV1a(obj,b,s)+r) mod W] (ring expanded by per-backend weight, deterministic skip to distinct backend) on healthy backends sorted by id; locations frozen in commit record\n")
	for b := 0; b < nBlocks; b++ {
		rec, ok := s.recordFor(objectID, b, k)
		if !ok {
			continue
		}
		fmt.Fprintf(&sb, " block %d: size=%d crc=%08x\n", b, rec.Size, rec.Checksum)
		for _, loc := range rec.Locs {
			path := VersionedShardPath(rec.ObjectID, rec.Block, loc.Shard, loc.Replica, rec.Version, rec.Writer)
			fmt.Fprintf(&sb, "   shard=%d replica=%d backend=%s path=%s shardCRC=%08x\n",
				loc.Shard, loc.Replica, loc.Backend, path, rec.ShardCRC[loc.Shard])
		}
	}
	return sb.String()
}

// replaceCommitted 用检查点快照整体替换内存提交视图（恢复时使用）。
// 记录按 Key 幂等折叠：重复恢复不会产生重复块，也不会引入快照之外的块。
func (s *Store) replaceCommitted(records []CommitRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fresh := map[string]map[int][]CommitRecord{}
	for _, rec := range records {
		blocks := fresh[rec.ObjectID]
		if blocks == nil {
			blocks = map[int][]CommitRecord{}
			fresh[rec.ObjectID] = blocks
		}
		dup := false
		for _, ex := range blocks[rec.Block] {
			if ex.Key() == rec.Key() {
				dup = true
				break
			}
		}
		if !dup {
			blocks[rec.Block] = append(blocks[rec.Block], rec)
		}
	}
	s.committed = fresh
	s.pending = map[string]uint32{} // opMu 写锁下理论上无在途写入，清空以防泄漏
}

// Backend 按 ID 返回后端实例（测试用于故障注入与检查）。
func (s *Store) Backend(id string) *DiskBackend { return s.backends[id] }

// BackendIDs 返回当前后端 ID（排序，确定性）。
func (s *Store) BackendIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.backends))
	for id := range s.backends {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// AddBackend 在运行中加入一个新后端。
// 只扩容、不搬迁：已有提交记录的位置不变，读取与恢复流程完全兼容；
// 新写入立即使用包含新后端的确定性布局。旧数据的搬迁由 Rebalance 完成。
// 持有 opMu 写锁，与写入/GC/恢复互斥，保证规划器切换是原子的。
func (s *Store) AddBackend(b *DiskBackend) error {
	if b == nil || b.id == "" {
		return errors.New("store: backend must have non-empty id")
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.backends[b.id]; dup {
		return fmt.Errorf("store: duplicate backend id %q", b.id)
	}
	s.backends[b.id] = b
	all := make([]*DiskBackend, 0, len(s.backends))
	for _, back := range s.backends {
		all = append(all, back)
	}
	s.planner = NewPlanner(all, s.cfg.Shards, s.cfg.Replicas)
	return nil
}

// applyRelocation 把一条重定位记录落到 WAL 并更新内存提交视图。
// 调用方必须保证：新位置上的副本已经完整落盘并通过 CRC 校验，
// 否则提交视图会指向不存在的数据（本包内仅 Rebalance 满足该前置条件）。
func (s *Store) applyRelocation(rec CommitRecord) error {
	if err := s.wal.Append(rec); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	recs := s.committed[rec.ObjectID][rec.Block]
	for i := range recs {
		if recs[i].Key() == rec.Key() {
			recs[i].Locs = append([]ReplicaLoc(nil), rec.Locs...)
		}
	}
	return nil
}
