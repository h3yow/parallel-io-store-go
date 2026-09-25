package store

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Backend 表示一个用本地磁盘目录模拟的存储后端。
// 一个后端 = 一个可独立注入故障的磁盘目录。
type Backend interface {
	Name() string
	WriteBlock(blockID string, data []byte) error
	ReadBlock(blockID string) ([]byte, error)
	RemoveBlock(blockID string) error
}

const (
	// blockMagic 是块文件头，便于识别与校验。
	blockMagic  = "PIOSBLK1\n"
	blockSuffix = ".dat"
	tmpPrefix   = ".tmp."
)

// Checksum 返回数据的 sha256 十六进制摘要。
// 块文件名中内嵌该校验和，实现内容寻址：同一块位置的新数据会产生新文件名，
// 旧文件自然成为垃圾，由 GC 依据存活检查点回收。
func Checksum(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// shortChecksum 取摘要前 16 个十六进制字符用于文件名。
func shortChecksum(data []byte) string {
	full := Checksum(data)
	return full[:16]
}

// DiskBackend 是 Backend 的本地磁盘实现。
// 块文件布局：
//
//	<dir>/<blockID>.dat
//	文件内容 = "PIOSBLK1\n" + <sha256 全hex>\n + <原始数据>
//
// 写入采用 “临时文件 + fsync + 原子 rename”，因此：
//   - 目录中永远不会出现只写了一半的块文件；
//   - 两个写入者并发写同名文件时，两个版本都先各自落在唯一临时文件上，
//     rename 后其中一个完整版本原子生效，不会产生拼接损坏。
type DiskBackend struct {
	name string
	dir  string

	mu          sync.RWMutex
	unavailable bool // 后端整体不可用（读写均失败）
	failWrites  bool // 仅注入写失败（模拟写入中途失败）
	failReads   bool // 仅注入读失败
}

// NewDiskBackend 在 dir 下准备（必要时创建）一个后端目录。
func NewDiskBackend(name, dir string) (*DiskBackend, error) {
	if name == "" || dir == "" {
		return nil, fmt.Errorf("%w: backend name/dir must not be empty", ErrInvalidArgument)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("%w: create backend %q dir: %v", ErrBackendIO, name, err)
	}
	return &DiskBackend{name: name, dir: dir}, nil
}

func (b *DiskBackend) Name() string      { return b.name }
func (b *DiskBackend) Directory() string { return b.dir }

func (b *DiskBackend) SetUnavailable(v bool) { b.mu.Lock(); b.unavailable = v; b.mu.Unlock() }
func (b *DiskBackend) SetFailWrites(v bool)  { b.mu.Lock(); b.failWrites = v; b.mu.Unlock() }
func (b *DiskBackend) SetFailReads(v bool)   { b.mu.Lock(); b.failReads = v; b.mu.Unlock() }

func (b *DiskBackend) IsAvailable() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return !b.unavailable
}

func (b *DiskBackend) path(blockID string) string {
	return filepath.Join(b.dir, blockID+blockSuffix)
}

// WriteBlock 原子写入一个块。
func (b *DiskBackend) WriteBlock(blockID string, data []byte) error {
	b.mu.RLock()
	unavail, failW := b.unavailable, b.failWrites
	b.mu.RUnlock()
	if unavail {
		return fmt.Errorf("%w: backend=%q", ErrBackendUnavailable, b.name)
	}
	if failW {
		// 模拟“写入中途失败”：临时文件可能残留，但永远不会被 rename 成正式文件。
		return fmt.Errorf("%w: backend=%q injected write failure for block=%s",
			ErrBackendIO, b.name, blockID)
	}
	if strings.ContainsAny(blockID, string(filepath.Separator)+"/") {
		return fmt.Errorf("%w: illegal block id %q", ErrInvalidArgument, blockID)
	}

	var randBytes [8]byte
	if _, err := rand.Read(randBytes[:]); err != nil {
		return fmt.Errorf("%w: backend=%q temp id: %v", ErrBackendIO, b.name, err)
	}
	tmp := filepath.Join(b.dir, fmt.Sprintf("%s%s.%x", tmpPrefix, blockID, randBytes[:]))

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("%w: backend=%q open temp: %v", ErrBackendIO, b.name, err)
	}
	// 即便后续步骤失败也要关闭并清理临时文件，绝不允许半成品以正式名出现。
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmp)
		}
	}()

	header := blockMagic + Checksum(data) + "\n"
	if _, err := f.WriteString(header); err != nil {
		f.Close()
		return fmt.Errorf("%w: backend=%q write header: %v", ErrBackendIO, b.name, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("%w: backend=%q write data: %v", ErrBackendIO, b.name, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("%w: backend=%q fsync: %v", ErrBackendIO, b.name, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("%w: backend=%q close: %v", ErrBackendIO, b.name, err)
	}
	if err := os.Rename(tmp, b.path(blockID)); err != nil {
		return fmt.Errorf("%w: backend=%q rename: %v", ErrBackendIO, b.name, err)
	}
	cleanup = false
	return nil
}

// ReadBlock 读取并验证块；校验失败返回包装了 ErrChecksumMismatch 的错误。
func (b *DiskBackend) ReadBlock(blockID string) ([]byte, error) {
	b.mu.RLock()
	unavail, failR := b.unavailable, b.failReads
	b.mu.RUnlock()
	if unavail {
		return nil, fmt.Errorf("%w: backend=%q", ErrBackendUnavailable, b.name)
	}
	if failR {
		return nil, fmt.Errorf("%w: backend=%q injected read failure for block=%s",
			ErrBackendIO, b.name, blockID)
	}
	raw, err := os.ReadFile(b.path(blockID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: backend=%q block=%s missing: %v",
				ErrBackendIO, b.name, blockID, err)
		}
		return nil, fmt.Errorf("%w: backend=%q read block=%s: %v",
			ErrBackendIO, b.name, blockID, err)
	}
	if len(raw) < len(blockMagic) || string(raw[:len(blockMagic)]) != blockMagic {
		return nil, fmt.Errorf("%w: backend=%q block=%s bad magic",
			ErrChecksumMismatch, b.name, blockID)
	}
	rest := raw[len(blockMagic):]
	nl := strings.IndexByte(string(rest), '\n')
	if nl < 0 {
		return nil, fmt.Errorf("%w: backend=%q block=%s missing checksum header",
			ErrChecksumMismatch, b.name, blockID)
	}
	wantSum := string(rest[:nl])
	data := rest[nl+1:]
	if got := Checksum(data); got != wantSum {
		return nil, fmt.Errorf("%w: backend=%q block=%s want=%s got=%s",
			ErrChecksumMismatch, b.name, blockID, wantSum, got)
	}
	return data, nil
}

// RemoveBlock 删除一个块文件；不存在不算错误（GC 需幂等）。
func (b *DiskBackend) RemoveBlock(blockID string) error {
	if err := os.Remove(b.path(blockID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: backend=%q remove block=%s: %v",
			ErrBackendIO, b.name, blockID, err)
	}
	return nil
}

// Exists 报告正式块文件是否存在（用于恢复时探测悬空副本）。
func (b *DiskBackend) Exists(blockID string) bool {
	_, err := os.Stat(b.path(blockID))
	return err == nil
}

// ListBlocks 列出目录下全部正式块文件名（不含临时文件、不含扩展名）。
func (b *DiskBackend) ListBlocks() ([]string, error) {
	entries, err := os.ReadDir(b.dir)
	if err != nil {
		return nil, fmt.Errorf("%w: backend=%q list: %v", ErrBackendIO, b.name, err)
	}
	var ids []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || strings.HasPrefix(name, tmpPrefix) || !strings.HasSuffix(name, blockSuffix) {
			continue
		}
		ids = append(ids, strings.TrimSuffix(name, blockSuffix))
	}
	sort.Strings(ids)
	return ids, nil
}

// QuarantineBlock 将一个块文件改名隔离（加 .quarantine-<rand> 后缀），
// 使其不再被任何读取或 GC 存活集合发现，但仍保留在磁盘上供排查。
func (b *DiskBackend) QuarantineBlock(blockID string) (string, error) {
	var rb [4]byte
	if _, err := rand.Read(rb[:]); err != nil {
		return "", err
	}
	old := b.path(blockID)
	newPath := filepath.Join(b.dir, blockID+blockSuffix+
		".quarantine-"+hex.EncodeToString(rb[:]))
	if err := os.Rename(old, newPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", err
	}
	return filepath.Base(newPath), nil
}

// CorruptBlock 破坏一个块文件的数据区（翻转一个数据字节），用于测试损坏检测。
// 若文件过短则截断一个字节。返回是否实施了破坏。
func (b *DiskBackend) CorruptBlock(blockID string) (bool, error) {
	p := b.path(blockID)
	f, err := os.OpenFile(p, os.O_RDWR, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return false, err
	}
	size := info.Size()
	if size <= int64(len(blockMagic))+64 {
		// 太小无法安全翻转数据区：截断 1 字节，同样会导致校验失败。
		if err := f.Truncate(size - 1); err != nil {
			return false, err
		}
		return true, nil
	}
	off := size - 1 // 数据区最后一个字节
	buf := make([]byte, 1)
	if _, err := f.ReadAt(buf, off); err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	buf[0] ^= 0xFF
	if _, err := f.WriteAt(buf, off); err != nil {
		return false, err
	}
	return true, f.Sync()
}

// ensure DiskBackend satisfies Backend.
var _ Backend = (*DiskBackend)(nil)
