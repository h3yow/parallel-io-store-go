package store

// 本文件定义"存储后端"抽象以及基于本地磁盘目录的实现。
// 每个后端对应一个本地目录，支持故障注入（写入失败 / 整体下线 / 写入损坏）。

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FaultKind 故障注入种类。
type FaultKind int

const (
	FaultNone      FaultKind = iota
	FaultWriteFail           // 接下来的写操作返回错误（模拟写中途失败）
	FaultOffline             // 后端整体不可用（网络断开/掉盘）
	FaultCorrupt             // 写入的数据被静默损坏（写完但字节被翻转）
)

// Backend 是单个存储后端的最小能力集。
type Backend interface {
	ID() string
	Available() bool
	// Write 以"临时文件 + fsync + rename"的原子语义写入完整文件：
	// 任何时刻磁盘上要么是旧的完整文件，要么是新的完整文件，
	// 不存在只写了一半的文件。
	Write(path string, data []byte) error
	Read(path string) ([]byte, error)
	Remove(path string) error
	// List 列出目录下的条目名（非递归）。
	List(dir string) ([]string, error)
	// Exists 判断文件是否存在。
	Exists(path string) bool
	// SizeBytes 返回文件字节数，不存在返回 0。
	SizeBytes(path string) int64
	// InjectFault 注入故障：FaultWriteFail/FaultCorrupt 作用于接下来 n 次写；
	// FaultOffline 为持续性故障，需显式 ResetFaults 恢复。
	InjectFault(kind FaultKind, n int)
	ResetFaults()
}

// errInjectedWrite 表示故障注入导致的写入失败（Store 层会包装为
// ShardWriteError，调用方可与后端不可用区分）。
var errInjectedWrite = errors.New("injected write failure")

// DiskBackend 基于本地目录的后端实现。
type DiskBackend struct {
	mu             sync.RWMutex
	id             string
	root           string
	fault          FaultKind
	faultRemaining int // FaultCorrupt / FaultWriteFail 的剩余次数
	budget         int64
	weight         int
}

// NewDiskBackend 创建一个磁盘后端，root 为其专属目录（不存在则创建）。
func NewDiskBackend(id, root string) (*DiskBackend, error) {
	if id == "" {
		return nil, errors.New("backend id must not be empty")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("backend %s: create root: %w", id, err)
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	return &DiskBackend{id: id, root: abs, fault: FaultNone, weight: 1}, nil
}

func (b *DiskBackend) ID() string { return b.id }

func (b *DiskBackend) rootDir() string { return b.root }

func (b *DiskBackend) Available() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.fault == FaultOffline {
		return false
	}
	return true
}

// peekFault 返回下一次写操作将表现的故障（不消耗计数）。
func (b *DiskBackend) peekFault() (FaultKind, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.fault == FaultOffline {
		return FaultOffline, false
	}
	if (b.fault == FaultCorrupt || b.fault == FaultWriteFail) && b.faultRemaining > 0 {
		return b.fault, true
	}
	return FaultNone, false
}

// consumeFault 消耗一次计数型故障配额。
func (b *DiskBackend) consumeFault() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.faultRemaining > 0 {
		b.faultRemaining--
		if b.faultRemaining == 0 {
			b.fault = FaultNone
		}
	}
}

func (b *DiskBackend) Write(path string, data []byte) error {
	kind, hit := b.peekFault()
	if kind == FaultOffline {
		return fmt.Errorf("%w: backend=%s", ErrBackendUnavailable, b.id)
	}
	if hit && kind == FaultWriteFail {
		b.consumeFault()
		return fmt.Errorf("%w: backend=%s path=%s", errInjectedWrite, b.id, path)
	}
	corrupt := hit && kind == FaultCorrupt
	if corrupt {
		b.consumeFault()
	}
	full := filepath.Join(b.root, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return fmt.Errorf("backend %s: mkdir: %w", b.id, err)
	}
	payload := data
	if corrupt {
		// 仅损坏副本字节，不影响调用方内存中的数据。
		payload = flipBytes(data)
	}
	tmp, err := os.CreateTemp(filepath.Dir(full), ".tmp-*")
	if err != nil {
		return fmt.Errorf("backend %s: temp file: %w", b.id, err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("backend %s: write: %w", b.id, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("backend %s: fsync: %w", b.id, err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("backend %s: close: %w", b.id, err)
	}
	if err := os.Rename(tmpName, full); err != nil {
		cleanup()
		return fmt.Errorf("backend %s: rename: %w", b.id, err)
	}
	// fsync 父目录，保证 rename 在崩溃后仍持久。
	if df, err := os.Open(filepath.Dir(full)); err == nil {
		_ = df.Sync()
		_ = df.Close()
	}
	return nil
}

// flipBytes 返回 data 的一份被确定性翻转的拷贝（至少翻转一个字节）。
func flipBytes(data []byte) []byte {
	if len(data) == 0 {
		return data
	}
	out := append([]byte(nil), data...)
	out[0] ^= 0xFF
	return out
}

func (b *DiskBackend) Read(path string) ([]byte, error) {
	if !b.Available() {
		return nil, fmt.Errorf("%w: backend=%s", ErrBackendUnavailable, b.id)
	}
	data, err := os.ReadFile(filepath.Join(b.root, filepath.FromSlash(path)))
	if err != nil {
		return nil, fmt.Errorf("backend %s: read %s: %w", b.id, path, err)
	}
	return data, nil
}

func (b *DiskBackend) Remove(path string) error {
	err := os.Remove(filepath.Join(b.root, filepath.FromSlash(path)))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("backend %s: remove %s: %w", b.id, path, err)
	}
	return nil
}

func (b *DiskBackend) List(dir string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(b.root, filepath.FromSlash(dir)))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("backend %s: list %s: %w", b.id, dir, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names, nil
}

func (b *DiskBackend) Exists(path string) bool {
	_, err := os.Stat(filepath.Join(b.root, filepath.FromSlash(path)))
	return err == nil
}

func (b *DiskBackend) SizeBytes(path string) int64 {
	fi, err := os.Stat(filepath.Join(b.root, filepath.FromSlash(path)))
	if err != nil {
		return 0
	}
	return fi.Size()
}

func (b *DiskBackend) InjectFault(kind FaultKind, n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.fault = kind
	if kind == FaultOffline {
		b.faultRemaining = 0
	} else {
		b.faultRemaining = n
	}
}

func (b *DiskBackend) ResetFaults() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.fault = FaultNone
	b.faultRemaining = 0
}
