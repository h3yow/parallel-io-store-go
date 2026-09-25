package store

// 本文件实现块提交的只追加 WAL（write-ahead commit record）。
// 每个块在"所有分片的所有副本全部落盘"之后，追加一条带校验和的提交记录。
// 读取时：只有在 WAL 中存在提交记录的块才可见 —— 因此绝不会读到
// 只写了一半的条带，也不会读到悬空副本（已写但未提交的分片在 GC 时回收）。
//
// 帧格式（小端，单条记录长度有上限）：
//
//	magic(4="PWAL") | payloadLen(4) | payload | crc32_castagnoli(4)
//
// payload 为记录字段的 TLV 序列化；记录还带有每个分片内容的 CRC，
// 供检查点验证时发现"静默损坏"的副本。
//
// 提交记录的去重键为 (object, block, version, writer)，
// 恢复时重复追加的相同记录幂等折叠，保证中断/重复恢复后不重不丢。

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"sync"
)

// ReplicaLoc 持久化一个分片副本提交时所在的后端。
// 放置一旦随提交记录持久化即固定：之后健康环成员变化也不会
// 改变该副本的寻址位置，避免"错放/漂移"。
type ReplicaLoc struct {
	Shard   int
	Replica int
	Backend string
}

// CommitRecord 是一条块提交记录。
type CommitRecord struct {
	ObjectID string
	Block    int
	Version  int64
	Writer   string
	Size     int          // 该块逻辑有效字节数（最后一块可能不足一块）
	Checksum uint32       // 块原始（重编码后）数据的 CRC32-IEEE
	ShardCRC []uint32     // 每个分片内容的 CRC32，顺序与分片号一致
	Locs     []ReplicaLoc // 每个 (分片, 副本) 提交时所在后端
}

// Key 返回幂等去重键。
func (r CommitRecord) Key() string {
	return fmt.Sprintf("%s\x00%d\x00%d\x00%s", r.ObjectID, r.Block, r.Version, r.Writer)
}

// equalPayload 判断两条同 Key 记录的载荷是否一致（幂等 vs 冲突的判定依据）。
func (r CommitRecord) equalPayload(o CommitRecord) bool {
	if r.Size != o.Size || r.Checksum != o.Checksum || len(r.ShardCRC) != len(o.ShardCRC) || len(r.Locs) != len(o.Locs) {
		return false
	}
	for i := range r.ShardCRC {
		if r.ShardCRC[i] != o.ShardCRC[i] {
			return false
		}
	}
	for i := range r.Locs {
		if r.Locs[i] != o.Locs[i] {
			return false
		}
	}
	return true
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// CommitWAL 是单个存储实例的提交日志。
type CommitWAL struct {
	mu      sync.Mutex
	back    Backend
	path    string
	seen    map[string]CommitRecord // 本进程内已追加的记录（幂等 + 冲突检测）
	records []CommitRecord          // 首次出现顺序
}

// NewCommitWAL 打开（不存在则创建）提交日志并立即重放恢复内存索引。
// 若文件尾部存在撕裂/损坏帧，会立即压缩重写为只含完好帧的规范内容，
// 避免坏尾帧永久截断后续新提交。
func NewCommitWAL(back Backend, path string) (*CommitWAL, error) {
	w := &CommitWAL{back: back, path: path, seen: map[string]CommitRecord{}}
	recs, validLen, err := w.replayBytes()
	if err != nil {
		return nil, err
	}
	raw, readErr := back.Read(path)
	if readErr == nil && validLen < len(raw) {
		// 存在被忽略的坏尾帧：用完好帧压缩重写（原子 rename）。
		canonical := w.encodeFrames(recs)
		if err := back.Write(path, canonical); err != nil {
			return nil, fmt.Errorf("wal compact: %w", err)
		}
	}
	w.records = recs
	for _, r := range recs {
		w.seen[r.Key()] = r
	}
	return w, nil
}

// encodeFrames 把记录列表编码为日志字节序列。
func (w *CommitWAL) encodeFrames(recs []CommitRecord) []byte {
	var out bytes.Buffer
	for _, rec := range recs {
		payload := rec.encodeTLV()
		out.WriteString("PWAL")
		var lb [4]byte
		binary.LittleEndian.PutUint32(lb[:], uint32(len(payload)))
		out.Write(lb[:])
		out.Write(payload)
		var cb [4]byte
		binary.LittleEndian.PutUint32(cb[:], crc32.Checksum(payload, castagnoli))
		out.Write(cb[:])
	}
	return out.Bytes()
}

// encodeTLV 将记录编码为自描述 TLV payload。
func (r CommitRecord) encodeTLV() []byte {
	var buf bytes.Buffer
	putStr := func(tag byte, s string) {
		buf.WriteByte(tag)
		var lb [4]byte
		binary.LittleEndian.PutUint32(lb[:], uint32(len(s)))
		buf.Write(lb[:])
		buf.WriteString(s)
	}
	putU64 := func(tag byte, v uint64) {
		buf.WriteByte(tag)
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], v)
		buf.Write(b[:])
	}
	putU32 := func(tag byte, v uint32) {
		buf.WriteByte(tag)
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], v)
		buf.Write(b[:])
	}
	putStr(1, r.ObjectID)
	putU64(2, uint64(r.Block))
	putU64(3, uint64(r.Version))
	putStr(4, r.Writer)
	putU64(5, uint64(r.Size))
	putU32(6, r.Checksum)
	for _, c := range r.ShardCRC {
		putU32(7, c)
	}
	for _, loc := range r.Locs {
		buf.WriteByte(8)
		var lb [4]byte
		binary.LittleEndian.PutUint32(lb[:], uint32(loc.Shard))
		buf.Write(lb[:])
		binary.LittleEndian.PutUint32(lb[:], uint32(loc.Replica))
		buf.Write(lb[:])
		var nb [2]byte
		binary.LittleEndian.PutUint16(nb[:], uint16(len(loc.Backend)))
		buf.Write(nb[:])
		buf.WriteString(loc.Backend)
	}
	return buf.Bytes()
}

// decodeTLV 解析记录；字段不全或越界即报错（损坏帧不会被当成提交）。
func decodeTLV(p []byte) (CommitRecord, error) {
	var r CommitRecord
	i := 0
	take := func(n int) ([]byte, error) {
		if i+n > len(p) {
			return nil, io.ErrUnexpectedEOF
		}
		b := p[i : i+n]
		i += n
		return b, nil
	}
	for i < len(p) {
		tag, err := take(1)
		if err != nil {
			return r, err
		}
		switch tag[0] {
		case 1, 4: // length-prefixed strings
			lb, err := take(4)
			if err != nil {
				return r, err
			}
			n := int(binary.LittleEndian.Uint32(lb))
			sb, err := take(n)
			if err != nil {
				return r, err
			}
			if tag[0] == 1 {
				r.ObjectID = string(sb)
			} else {
				r.Writer = string(sb)
			}
		case 2, 3, 5:
			b, err := take(8)
			if err != nil {
				return r, err
			}
			v := int64(binary.LittleEndian.Uint64(b))
			switch tag[0] {
			case 2:
				r.Block = int(binary.LittleEndian.Uint64(b))
			case 3:
				r.Version = v
			case 5:
				r.Size = int(binary.LittleEndian.Uint64(b))
			}
		case 6:
			b, err := take(4)
			if err != nil {
				return r, err
			}
			r.Checksum = binary.LittleEndian.Uint32(b)
		case 7:
			b, err := take(4)
			if err != nil {
				return r, err
			}
			r.ShardCRC = append(r.ShardCRC, binary.LittleEndian.Uint32(b))
		case 8:
			hdr, err := take(10)
			if err != nil {
				return r, err
			}
			n := int(binary.LittleEndian.Uint16(hdr[8:10]))
			name, err := take(n)
			if err != nil {
				return r, err
			}
			r.Locs = append(r.Locs, ReplicaLoc{
				Shard:   int(binary.LittleEndian.Uint32(hdr[0:4])),
				Replica: int(binary.LittleEndian.Uint32(hdr[4:8])),
				Backend: string(name),
			})
		default:
			return r, fmt.Errorf("unknown TLV tag %d", tag[0])
		}
	}
	if r.ObjectID == "" || r.Writer == "" {
		return r, errors.New("record missing required fields")
	}
	return r, nil
}

// Append 追加一条提交记录。相同 Key 且载荷相同则幂等返回；
// 相同 Key 不同载荷返回 ErrConflict（确定性，不写入）。
func (w *CommitWAL) Append(rec CommitRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if old, ok := w.seen[rec.Key()]; ok {
		if !old.equalPayload(rec) {
			return fmt.Errorf("%w: key=%s", ErrConflict, rec.Key())
		}
		return nil
	}
	payload := rec.encodeTLV()
	var frame bytes.Buffer
	frame.WriteString("PWAL")
	var lb [4]byte
	binary.LittleEndian.PutUint32(lb[:], uint32(len(payload)))
	frame.Write(lb[:])
	frame.Write(payload)
	var cb [4]byte
	binary.LittleEndian.PutUint32(cb[:], crc32.Checksum(payload, castagnoli))
	frame.Write(cb[:])

	// 用"读全量 + 追加后原子重写"的方式保证崩溃时日志文件要么含旧帧、
	// 要么含全部新帧，不会出现撕裂尾帧（重放时撕裂帧被安全忽略）。
	old, _ := w.back.Read(w.path)
	combined := append(append([]byte(nil), old...), frame.Bytes()...)
	if err := w.back.Write(w.path, combined); err != nil {
		return err
	}
	w.seen[rec.Key()] = rec
	w.records = append(w.records, rec)
	return nil
}

// Replay 读取全部记录，按 Key 折叠重复项；
// 校验和错误/不完整的尾帧被跳过（当作未提交），保证损坏日志不会伪造提交。
func (w *CommitWAL) Replay() ([]CommitRecord, error) {
	recs, _, err := w.replayBytes()
	return recs, err
}

// replayBytes 重放日志，返回折叠后的记录与最后一个完好帧的结束偏移。
func (w *CommitWAL) replayBytes() ([]CommitRecord, int, error) {
	data, err := w.back.Read(w.path)
	if err != nil {
		if errors.Is(err, ErrBackendUnavailable) {
			return nil, 0, err
		}
		return nil, 0, nil // 文件尚不存在
	}
	seen := map[string]CommitRecord{}
	var out []CommitRecord
	i := 0
	for i < len(data) {
		if len(data)-i < 12 || string(data[i:i+4]) != "PWAL" {
			break // 撕裂/未知头：停止重放
		}
		plen := int(binary.LittleEndian.Uint32(data[i+4 : i+8]))
		end := i + 12 + plen
		if plen < 0 || end > len(data) {
			break // 尾帧不完整
		}
		payload := data[i+8 : i+8+plen]
		want := binary.LittleEndian.Uint32(data[i+8+plen : end])
		if crc32.Checksum(payload, castagnoli) != want {
			break // 帧损坏：该帧及之后均不可信
		}
		rec, err := decodeTLV(payload)
		if err != nil {
			break
		}
		if old, ok := seen[rec.Key()]; ok {
			if !old.equalPayload(rec) {
				return nil, 0, fmt.Errorf("%w: conflicting on-disk records key=%s", ErrConflict, rec.Key())
			}
		} else {
			seen[rec.Key()] = rec
			out = append(out, rec)
		}
		i = end
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].ObjectID != out[j].ObjectID {
			return out[i].ObjectID < out[j].ObjectID
		}
		if out[i].Block != out[j].Block {
			return out[i].Block < out[j].Block
		}
		if out[i].Version != out[j].Version {
			return out[i].Version < out[j].Version
		}
		return out[i].Writer < out[j].Writer
	})
	return out, i, nil
}

// Records 返回已折叠的提交记录快照（按确定性顺序）。
func (w *CommitWAL) Records() []CommitRecord {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := append([]CommitRecord(nil), w.records...)
	return out
}

// Compact 用给定记录集合原子重写日志（检查点回退后使用）：
// 使"回退到某检查点"的决定在进程重启后依然成立——
// 该检查点之后的提交被确定性地丢弃（这正是回退语义），
// 其分片随后由 GC 回收。
func (w *CommitWAL) Compact(records []CommitRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	sorted := append([]CommitRecord(nil), records...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Key() < sorted[j].Key() })
	if err := w.back.Write(w.path, w.encodeFrames(sorted)); err != nil {
		return err
	}
	w.records = sorted
	w.seen = map[string]CommitRecord{}
	for _, r := range sorted {
		w.seen[r.Key()] = r
	}
	return nil
}

// Sync 显式刷盘（Write 路径本身已做 fsync，此方法保留给接口完整性）。
func (w *CommitWAL) Sync() error { return nil }

// Close 关闭日志（当前实现无额外资源）。
func (w *CommitWAL) Close() error { return nil }

var _ io.Closer = (*CommitWAL)(nil)
