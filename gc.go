package store

// 本文件实现空间回收：
//   - 以"当前生效的已提交块集合（WAL 视图 / 最近一次恢复后的检查点视图）"
//     为存活根集；
//   - 精确存活集合 = 所有存活提交记录 Locs 指向的
//     (backend, VersionedShardPath)；
//   - 在各后端上枚举 shards/ 下的全部 .data 文件，不在存活集合内的即垃圾：
//     写入中途失败的悬空副本、被更新快照取代的旧版本分片、损坏检查点
//     时代的孤儿文件；
//   - GC 可在恢复后任意重复运行，结果幂等；
//   - GC 后占用空间与存活数据严格一致（SpaceUsage 可供断言）。

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// GCReport 是一次回收的可解释报告。
type GCReport struct {
	DeletedFiles []string
	DeletedBytes int64
	KeptFiles    int
	KeptBytes    int64
	Reason       string
}

// liveShardFiles 计算存活分片文件集合：backendID -> 相对路径集合。
func (s *Store) liveShardFiles() map[string]map[string]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	live := map[string]map[string]struct{}{}
	add := func(backendID, p string) {
		if live[backendID] == nil {
			live[backendID] = map[string]struct{}{}
		}
		live[backendID][p] = struct{}{}
	}
	for _, blocks := range s.committed {
		for _, recs := range blocks {
			for _, rec := range recs {
				for _, loc := range rec.Locs {
					p := VersionedShardPath(rec.ObjectID, rec.Block, loc.Shard, loc.Replica, rec.Version, rec.Writer)
					add(loc.Backend, p)
				}
			}
		}
	}
	return live
}

// RunGC 回收所有不属于存活块集合的分片文件。
// 持有 opMu 写锁，与并发块写入互斥，确保枚举磁盘期间不会有
// "已落盘但尚未进入存活索引"的新副本被误删。
func (s *Store) RunGC() (*GCReport, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	live := s.liveShardFiles()
	rep := &GCReport{}

	// 按后端 ID 排序，保证删除动作与报告可复现。
	ids := make([]string, 0, len(s.backends))
	for id := range s.backends {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		back := s.backends[id]
		root := filepath.Join(back.rootDir(), "shards")
		walkErr := filepath.Walk(root, func(full string, info os.FileInfo, err error) error {
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			if info.IsDir() || filepath.Ext(full) != ".data" {
				return nil
			}
			rel, rerr := filepath.Rel(back.rootDir(), full)
			if rerr != nil {
				return rerr
			}
			rel = filepath.ToSlash(rel)
			size := info.Size()
			if _, keep := live[id][rel]; keep {
				rep.KeptFiles++
				rep.KeptBytes += size
				return nil
			}
			if rmErr := back.Remove(rel); rmErr != nil {
				return rmErr
			}
			rep.DeletedFiles = append(rep.DeletedFiles, fmt.Sprintf("%s:%s", id, rel))
			rep.DeletedBytes += size
			return nil
		})
		if walkErr != nil {
			return nil, fmt.Errorf("gc: backend=%s: %w", id, walkErr)
		}
	}
	sort.Strings(rep.DeletedFiles)
	rep.Reason = fmt.Sprintf(
		"live set = exact (backend,path) pairs referenced by committed records in the current effective view; "+
			"deleted %d orphan/garbage shard files (%d bytes), kept %d files (%d bytes)",
		len(rep.DeletedFiles), rep.DeletedBytes, rep.KeptFiles, rep.KeptBytes)
	return rep, nil
}

// SpaceUsage 返回各后端上分片数据（shards/ 下 .data 文件）的字节占用。
func (s *Store) SpaceUsage() (map[string]int64, error) {
	out := map[string]int64{}
	ids := make([]string, 0, len(s.backends))
	for id := range s.backends {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		back := s.backends[id]
		var total int64
		root := filepath.Join(back.rootDir(), "shards")
		err := filepath.Walk(root, func(full string, info os.FileInfo, err error) error {
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			if !info.IsDir() && filepath.Ext(full) == ".data" {
				total += info.Size()
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("space usage: backend=%s: %w", id, err)
		}
		out[id] = total
	}
	return out, nil
}
