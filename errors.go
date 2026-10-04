package store

// 本文件定义存储层对外暴露的可区分错误类型。
// 调用方可以用 errors.Is / errors.As 区分失败原因，
// 从而满足"不得静默丢块、失败原因可区分"的要求。

import (
	"errors"
	"fmt"
)

var (
	// ErrReplicaShortage 可用后端数量不足以放置要求的副本数。
	ErrReplicaShortage = errors.New("replica shortage: not enough distinct healthy backends")
	// ErrCapacityExhausted 目标后端容量预算不足以容纳新副本。
	// 与 ErrReplicaShortage 可区分：前者是"位置够但放不下"，后者是"健康后端不够"。
	ErrCapacityExhausted = errors.New("capacity exhausted: backend budget cannot fit new replicas")
	// ErrBackendUnavailable 指定后端当前不可用（被故障注入下线等）。
	ErrBackendUnavailable = errors.New("backend unavailable")
	// ErrShardWriteFailed 某个分片副本写入失败（中途故障）。
	ErrShardWriteFailed = errors.New("shard replica write failed")
	// ErrBlockIncomplete 块的部分分片未能落盘，块未提交（读不可见）。
	ErrBlockIncomplete = errors.New("block incomplete: commit aborted")
	// ErrBlockDegraded 块已提交但当前存活副本不足以重建（无任何健康分片）。
	ErrBlockDegraded = errors.New("block degraded: no reconstructable shard")
	// ErrConflict 同一 (版本, 写入者) 被不同内容重复提交。
	ErrConflict = errors.New("write conflict: same version/writer with different payload")
	// ErrCheckpointCorrupt 检查点清单引用的分片损坏或缺失。
	ErrCheckpointCorrupt = errors.New("checkpoint corrupt")
	// ErrNoCheckpoint 不存在任何可用检查点。
	ErrNoCheckpoint = errors.New("no valid checkpoint")
	// ErrObjectNotFound 对象不存在。
	ErrObjectNotFound = errors.New("object not found")
)

// PlacementError 描述副本放置阶段的失败原因。
type PlacementError struct {
	ObjectID string
	Block    int
	Shard    int
	Want     int
	Healthy  int
	Cause    error
	Decision string // 判定依据的文字说明
}

func (e *PlacementError) Error() string {
	return fmt.Sprintf("placement failed object=%s block=%d shard=%d want=%d healthy=%d: %v (%s)",
		e.ObjectID, e.Block, e.Shard, e.Want, e.Healthy, e.Cause, e.Decision)
}

func (e *PlacementError) Unwrap() error { return e.Cause }

// ShardWriteError 描述单个分片副本写入失败。
type ShardWriteError struct {
	BackendID string
	ObjectID  string
	Block     int
	Shard     int
	Replica   int
	Cause     error
}

func (e *ShardWriteError) Error() string {
	return fmt.Sprintf("shard write failed backend=%s object=%s block=%d shard=%d replica=%d: %v",
		e.BackendID, e.ObjectID, e.Block, e.Shard, e.Replica, e.Cause)
}

func (e *ShardWriteError) Unwrap() error { return e.Cause }
