package store

// 本文件实现检查点机制：
//   - checkpoint 是某时刻全部已提交块（WAL 视图）的不可变 JSON manifest，
//     经"临时文件 + rename"原子发布，序号严格递增；
//   - CURRENT 指针指向最近一个"经验证完好"的检查点；
//   - 恢复时从 CURRENT 开始从新到旧逐个验证：引用的每个分片至少有一个
//     副本字节存在且 CRC 正确才算完好；损坏则记录判定依据并回退到上一个，
//     直到找到完好检查点或返回 ErrNoCheckpoint；
//   - 恢复可任意重复执行，结论幂等：数据不重复、不丢失。

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	checkpointDir  = "meta/checkpoints"
	currentPointer = "meta/CURRENT"
)

// CheckpointManifest 是检查点清单。
type CheckpointManifest struct {
	Seq         int64          `json:"seq"`
	CreatedUnix int64          `json:"created_unix"`
	Records     []CommitRecord `json:"records"`
}

// CheckpointResult 是恢复验证的可解释结论。
type CheckpointResult struct {
	SelectedSeq int64             // 最终选中的检查点序号；-1 表示无
	Tried       []CheckpointProbe // 从新到旧的探测过程
	LiveObjects []string          // 选中检查点包含的对象
	Reason      string            // 判定依据文字说明
}

// CheckpointProbe 记录对单个检查点的探测结果。
type CheckpointProbe struct {
	Seq       int64
	Healthy   bool
	Reason    string
	BadShards []BadShard
}

// BadShard 描述一个被判定损坏/缺失的分片位置。
type BadShard struct {
	ObjectID string `json:"object"`
	Block    int    `json:"block"`
	Shard    int    `json:"shard"`
	Backend  string `json:"backend"`
	Path     string `json:"path"`
	Reason   string `json:"reason"`
}

// Checkpointer 负责检查点的创建、发布与验证恢复。
type Checkpointer struct {
	store *Store
}

// NewCheckpointer 创建检查点管理器。
func NewCheckpointer(s *Store) *Checkpointer { return &Checkpointer{store: s} }

func (c *Checkpointer) manifestPath(seq int64) string {
	return path.Join(checkpointDir, fmt.Sprintf("cp-%010d.json", seq))
}

// Create 用当前已提交块生成一个新检查点并原子发布。
func (c *Checkpointer) Create() (int64, error) {
	s := c.store
	recs := s.wal.Records()
	sort.Slice(recs, func(i, j int) bool { return recs[i].Key() < recs[j].Key() })
	seq := time.Now().UnixNano()
	mf := CheckpointManifest{Seq: seq, CreatedUnix: time.Now().Unix(), Records: recs}
	raw, err := json.MarshalIndent(&mf, "", "  ")
	if err != nil {
		return 0, err
	}
	if err := s.metaBack.Write(c.manifestPath(seq), raw); err != nil {
		return 0, fmt.Errorf("checkpoint: write manifest: %w", err)
	}
	// 清单持久化成功后再翻转 CURRENT（原子指针）。
	if err := s.metaBack.Write(currentPointer, []byte(strconv.FormatInt(seq, 10))); err != nil {
		return 0, fmt.Errorf("checkpoint: publish CURRENT: %w", err)
	}
	return seq, nil
}

// readManifest 读取并解析指定序号的清单。
func (c *Checkpointer) readManifest(seq int64) (*CheckpointManifest, error) {
	raw, err := c.store.metaBack.Read(c.manifestPath(seq))
	if err != nil {
		return nil, err
	}
	var mf CheckpointManifest
	if err := json.Unmarshal(raw, &mf); err != nil {
		return nil, fmt.Errorf("%w: seq=%d: %v", ErrCheckpointCorrupt, seq, err)
	}
	return &mf, nil
}

// ListSeq 列出磁盘上存在的检查点序号（升序）。
func (c *Checkpointer) ListSeq() ([]int64, error) {
	names, err := c.store.metaBack.List(checkpointDir)
	if err != nil {
		return nil, nil // 目录尚不存在
	}
	var seqs []int64
	for _, n := range names {
		var seq int64
		if _, err := fmt.Sscanf(n, "cp-%d.json", &seq); err == nil {
			seqs = append(seqs, seq)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs, nil
}

// verifyManifest 逐条块、逐分片验证清单：每个分片至少一个副本字节完好。
// 同时验证重建后块 CRC，任何问题都记入返回的 BadShard/原因。
func (c *Checkpointer) verifyManifest(mf *CheckpointManifest) (bool, []BadShard, string) {
	var bad []BadShard
	for _, rec := range mf.Records {
		for si := 0; si < len(rec.ShardCRC); si++ {
			good := false
			var reasons []string
			for _, loc := range rec.Locs {
				if loc.Shard != si {
					continue
				}
				back := c.store.backends[loc.Backend]
				p := VersionedShardPath(rec.ObjectID, rec.Block, si, loc.Replica, rec.Version, rec.Writer)
				if back == nil || !back.Available() {
					reasons = append(reasons, fmt.Sprintf("backend=%s unavailable", loc.Backend))
					bad = append(bad, BadShard{rec.ObjectID, rec.Block, si, loc.Backend, p, "backend unavailable"})
					continue
				}
				data, err := back.Read(p)
				if err != nil {
					reasons = append(reasons, fmt.Sprintf("backend=%s read-error: %v", loc.Backend, err))
					bad = append(bad, BadShard{rec.ObjectID, rec.Block, si, loc.Backend, p, err.Error()})
					continue
				}
				if crc32.ChecksumIEEE(data) != rec.ShardCRC[si] {
					reasons = append(reasons, fmt.Sprintf("backend=%s crc-mismatch", loc.Backend))
					bad = append(bad, BadShard{rec.ObjectID, rec.Block, si, loc.Backend, p, "shard crc mismatch"})
					continue
				}
				good = true
				break
			}
			if !good {
				return false, bad, fmt.Sprintf("object=%s block=%d shard=%d has no intact replica: %s",
					rec.ObjectID, rec.Block, si, strings.Join(reasons, "; "))
			}
		}
	}
	return true, nil, "all referenced shards have at least one intact replica (CRC verified)"
}

// Restore 验证并回退到最近的完好检查点，并将内存提交视图重置到该快照。
// 持有 opMu 写锁：恢复期间不允许并发块写入，保证视图切换的原子性。
func (c *Checkpointer) Restore() (*CheckpointResult, error) {
	c.store.opMu.Lock()
	defer c.store.opMu.Unlock()
	seqs, err := c.ListSeq()
	if err != nil {
		return nil, err
	}
	result := &CheckpointResult{SelectedSeq: -1}
	if len(seqs) == 0 {
		result.Reason = "no checkpoint exists on disk"
		return result, fmt.Errorf("%w", ErrNoCheckpoint)
	}
	// 从新到旧尝试。
	for i := len(seqs) - 1; i >= 0; i-- {
		seq := seqs[i]
		mf, err := c.readManifest(seq)
		if err != nil {
			result.Tried = append(result.Tried, CheckpointProbe{Seq: seq, Healthy: false, Reason: err.Error()})
			continue
		}
		ok, bad, reason := c.verifyManifest(mf)
		probe := CheckpointProbe{Seq: seq, Healthy: ok, Reason: reason, BadShards: bad}
		result.Tried = append(result.Tried, probe)
		if !ok {
			continue // 损坏：回退到上一个检查点
		}
		// 选中：把 WAL 压缩到该检查点，使回退在重启后仍成立；
		// 再整体替换内存视图为快照内容（幂等：重复 Restore 结果相同）。
		if err := c.store.wal.Compact(mf.Records); err != nil {
			return nil, fmt.Errorf("checkpoint: compact wal to seq=%d: %w", seq, err)
		}
		c.store.replaceCommitted(mf.Records)
		live := map[string]struct{}{}
		for _, r := range mf.Records {
			live[r.ObjectID] = struct{}{}
		}
		for obj := range live {
			result.LiveObjects = append(result.LiveObjects, obj)
		}
		sort.Strings(result.LiveObjects)
		result.SelectedSeq = seq
		result.Reason = fmt.Sprintf("selected newest intact checkpoint seq=%d; %d committed blocks; older checkpoints not needed", seq, len(mf.Records))
		// 刷新 CURRENT 指向经确认的完好检查点（若它本就不是最新）。
		_ = c.store.metaBack.Write(currentPointer, []byte(strconv.FormatInt(seq, 10)))
		return result, nil
	}
	result.Reason = "all checkpoints failed verification; no intact checkpoint to fall back to"
	return result, fmt.Errorf("%w", ErrNoCheckpoint)
}
