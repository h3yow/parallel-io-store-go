package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ChunkRef 描述一个已落盘 chunk 副本的位置与校验和。
type ChunkRef struct {
	Backend  string `json:"backend"`
	File     string `json:"file"` // 后端目录中的块文件名（blockID）
	Checksum string `json:"checksum"`
	Length   int    `json:"length"`
}

// BlockEntry 描述一个逻辑块在某个检查点中的全部 chunk 及副本。
type BlockEntry struct {
	LogicalBlock uint64       `json:"logical_block"`
	Writer       string       `json:"writer"`
	DataChecksum string       `json:"data_checksum"` // 整个逻辑块数据的 sha256（仲裁与判重用）
	ChunkSize    int          `json:"chunk_size"`
	Length       int          `json:"length"`
	Chunks       [][]ChunkRef `json:"chunks"` // [chunkIndex][replicaIndex]
}

// Checkpoint 是一次原子提交的检查点清单（不可变快照）。
type Checkpoint struct {
	Seq       int                    `json:"seq"`
	WriterID  string                 `json:"writer_id"`
	CreatedAt string                 `json:"created_at"`
	Blocks    map[uint64]*BlockEntry `json:"blocks"`
}

const (
	cpPrefix = "checkpoint-"
	cpSuffix = ".json"
	cpWidth  = 12
)

func cpName(seq int) string {
	return fmt.Sprintf("%s%0*d%s", cpPrefix, cpWidth, seq, cpSuffix)
}

// manifestStore 负责检查点清单文件的原子读写与枚举。
// 所有写入都走 “临时文件 + fsync + 原子 rename + 目录 fsync”，
// 因此崩溃后要么看到完整的旧清单，要么看到完整的新清单，不存在半个 JSON。
type manifestStore struct {
	dir string
	mu  sync.Mutex
}

func newManifestStore(metaDir string) (*manifestStore, error) {
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		return nil, fmt.Errorf("%w: create meta dir: %v", ErrBackendIO, err)
	}
	return &manifestStore{dir: metaDir}, nil
}

func (m *manifestStore) Dir() string { return m.dir }

// save 原子落盘一个检查点文件。
func (m *manifestStore) save(cp *Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cp.Blocks == nil {
		cp.Blocks = map[uint64]*BlockEntry{}
	}
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("%w: marshal checkpoint seq=%d: %v", ErrBackendIO, cp.Seq, err)
	}
	if err := atomicWriteFile(filepath.Join(m.dir, cpName(cp.Seq)), data, 0o644); err != nil {
		return fmt.Errorf("%w: persist checkpoint seq=%d: %v", ErrBackendIO, cp.Seq, err)
	}
	return nil
}

// rewrite 用给定内容原地重写某个检查点文件（恢复修复：剔除坏副本引用）。
func (m *manifestStore) rewrite(path string, cp *Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("%w: marshal repaired checkpoint: %v", ErrBackendIO, err)
	}
	if err := atomicWriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("%w: rewrite checkpoint %s: %v", ErrBackendIO, path, err)
	}
	return nil
}

func (m *manifestStore) remove(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// list 返回全部检查点路径，按 seq 升序。
func (m *manifestStore) list() ([]string, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return nil, fmt.Errorf("%w: list meta dir: %v", ErrBackendIO, err)
	}
	var paths []string
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasPrefix(n, cpPrefix) || !strings.HasSuffix(n, cpSuffix) {
			continue
		}
		paths = append(paths, filepath.Join(m.dir, n))
	}
	sort.Slice(paths, func(i, j int) bool { return seqOf(paths[i]) < seqOf(paths[j]) })
	return paths, nil
}

func seqOf(path string) int {
	base := filepath.Base(path)
	body := strings.TrimSuffix(strings.TrimPrefix(base, cpPrefix), cpSuffix)
	var seq int
	fmt.Sscanf(body, "%d", &seq)
	return seq
}

// latest 返回 seq 最大的检查点；没有任何检查点时返回 (nil, nil)。
func (m *manifestStore) latest() (*Checkpoint, error) {
	paths, err := m.list()
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, nil
	}
	return m.load(paths[len(paths)-1])
}

func (m *manifestStore) load(path string) (*Checkpoint, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w: read checkpoint %s: %v", ErrBackendIO, path, err)
	}
	var cp Checkpoint
	if err := json.Unmarshal(raw, &cp); err != nil {
		return nil, fmt.Errorf("%w: parse checkpoint %s: %v", ErrBackendIO, path, err)
	}
	if cp.Blocks == nil {
		cp.Blocks = map[uint64]*BlockEntry{}
	}
	return &cp, nil
}

// cloneBlocks 深拷贝块表。
func cloneBlocks(src map[uint64]*BlockEntry) map[uint64]*BlockEntry {
	dst := make(map[uint64]*BlockEntry, len(src)+1)
	for k, v := range src {
		cp := *v
		cp.Chunks = make([][]ChunkRef, len(v.Chunks))
		for i := range v.Chunks {
			cp.Chunks[i] = append([]ChunkRef(nil), v.Chunks[i]...)
		}
		dst[k] = &cp
	}
	return dst
}

// atomicWriteFile 写入临时文件、fsync、原子 rename，并 fsync 所在目录。
func atomicWriteFile(path string, data []byte, perm os.FileMode) (err error) {
	dir := filepath.Dir(path)
	tmp := filepath.Join(dir, fmt.Sprintf(".tmp.%d-%d", time.Now().UnixNano(), os.Getpid()))
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_EXCL, perm)
	if err != nil {
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmp)
		}
	}()
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	cleanup = false
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
