package store

import (
	"errors"
	"fmt"
)

// 可区分的失败原因（哨兵错误）。
var (
	// ErrInsufficientBackends 在线后端数量不足以满足副本数要求。
	ErrInsufficientBackends = errors.New("insufficient healthy backends for requested replicas")
	// ErrBackendUnavailable 目标后端处于注入故障/不可用状态。
	ErrBackendUnavailable = errors.New("backend unavailable (injected failure)")
	// ErrBackendIO 后端在读写过程中发生 I/O 错误。
	ErrBackendIO = errors.New("backend io error")
	// ErrChecksumMismatch 块内容校验和与文件名/清单记录不一致（块损坏）。
	ErrChecksumMismatch = errors.New("block checksum mismatch (corruption)")
	// ErrPlacementConflict 并发写入仲裁失败：本写入不是胜出者。
	ErrPlacementConflict = errors.New("write lost arbitration against a newer write")
	// ErrNoCheckpoint 不存在任何完好的检查点。
	ErrNoCheckpoint = errors.New("no intact checkpoint available")
	// ErrReadOnly 存储已被显式关闭。
	ErrReadOnly = errors.New("store is closed")
	// ErrInvalidArgument 参数非法。
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrNotFound 逻辑块在当前检查点中不存在。
	ErrNotFound = errors.New("logical block not found")
)

// FailureStage 标识一次多副本写入在哪个阶段失败。
type FailureStage int

const (
	StageUnknown FailureStage = iota
	StagePlan
	StageReplicate
	StageCommit
)

// WriteError 携带一次写入失败的可解释细节：
// 阶段、逻辑块、哪些后端失败、封装的底层原因。
type WriteError struct {
	Stage          FailureStage
	LogicalBlock   uint64
	FailedBackends []string
	Err            error
}

func (e *WriteError) Error() string {
	if e == nil {
		return ""
	}
	name := map[FailureStage]string{
		StageUnknown:   "unknown",
		StagePlan:      "plan",
		StageReplicate: "replicate",
		StageCommit:    "commit",
	}[e.Stage]
	return fmt.Sprintf("write logical-block=%d stage=%s failed-backends=%v: %v",
		e.LogicalBlock, name, e.FailedBackends, e.Err)
}

func (e *WriteError) Unwrap() error { return e.Err }
