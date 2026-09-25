package store

import (
	"os"
	"path/filepath"
	"testing"
)

func backsIndex(backs []*DiskBackend, id string) int {
	for i, b := range backs {
		if b.ID() == id {
			return i
		}
	}
	return -1
}

// corruptFile 直接在后端目录中覆写目标文件内容，模拟静默损坏（幂等）。
func corruptFile(t *testing.T, b *DiskBackend, rel string) {
	t.Helper()
	full := filepath.Join(b.rootDir(), filepath.FromSlash(rel))
	data, err := os.ReadFile(full)
	if err != nil {
		t.Fatalf("read file to corrupt: %v", err)
	}
	for i := range data {
		data[i] = 0xFF
	}
	if err := os.WriteFile(full, data, 0o644); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}
}
