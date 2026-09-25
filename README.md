# 本地并行存储层（Parallel I/O Store）

用**本地磁盘目录**模拟多个相互独立的存储后端，在其上实现条带化写入、多副本放置、
确定性并发仲裁、检查点/恢复与空间回收。仅依赖 Go 标准库与本地文件系统能力，
不依赖任何外部服务或真实设备。

## 1. 目录布局

```
<RootDir>/
  backend-<name1>/           # 模拟后端 1（可独立注入故障）
  backend-<name2>/
  ...
  meta/
    checkpoint-000000000001.json
    checkpoint-000000000002.json
```

- 每个 chunk 副本是后端目录中的一个文件：`b<逻辑块号>-c<chunk号>-<sha256前16位>.dat`。
- 块文件内容为 `PIOSBLK1\n<完整sha256>\n<数据>`，读取时重新计算 sha256 并比对，
  任何字节损坏都会被发现（`ErrChecksumMismatch`）。
- 文件名内嵌内容摘要，即**内容寻址**：同一 chunk 位置的新数据产生新文件名，
  旧文件自然成为垃圾，由 GC 依据存活检查点回收；同一内容在多个检查点间共享同一份文件。

## 2. 分片与副本规则（确定性）

设后端按名字排序后编号为 `0..N-1`，逻辑块号为 `L`，chunk 大小为 `C`：

```
chunk 数      = ceil(len(data) / C)
base(i)       = (L + i) mod N                     # 第 i 个 chunk 的条带起点
backend(i, r) = sortedBackends[(base(i) + r) mod N]   # 第 r 个副本
```

性质：

1. **完全确定、可复现**：给定后端集合、逻辑块号与数据，布局永远相同，与 goroutine
   调度无关；测试日志中的 `Explain()` 会打印每个 chunk/副本的后端与判定依据。
2. **条带化轮转**：相邻 chunk 的主副本起点轮转，负载均匀分散到所有后端。
3. **副本分散**：当 `replicas <= N` 时，同一 chunk 的副本落在互不相同的后端。
4. **绝不静默降配**：规划阶段若健康后端数 `< replicas`，立即返回
   `ErrInsufficientBackends`（并列出不可用后端），不会少放副本或错放副本。

### 写入协议（三阶段，单提交点）

1. **Plan**：计算确定性布局并校验健康后端数量；
2. **Replicate**：所有 chunk 的全部副本**并行**写入各自后端。写入采用
   “临时文件 + fsync + 原子 rename”，因此任何时刻目录里只有“完整的旧版本”或
   “完整的新版本”，不存在半个块；
3. **Commit**：全部副本成功后，才生成**新的不可变检查点**并原子落盘（旧检查点不变）。

任一副本失败则整次写入中止：本次已上传的副本立即回滚删除（但不删除旧检查点仍引用
的同名文件），检查点不前进。错误类型 `WriteError` 携带失败阶段
（`StagePlan/StageReplicate/StageCommit`）与失败后端列表，失败原因可区分：

| 现象 | 错误 |
|---|---|
| 健康后端不足 | `ErrInsufficientBackends`（StagePlan） |
| 后端被置为不可用 | `ErrBackendUnavailable` |
| 写入/读取 I/O 失败（含注入的“中途失败”） | `ErrBackendIO` |
| 块内容/摘要不符（损坏） | `ErrChecksumMismatch` |
| 并发写入仲裁落败 | `ErrPlacementConflict`（StageCommit） |

## 3. 并发一致性

- **同一逻辑块串行化**：按逻辑块号加锁，消除半成品交错，且使仲裁与调度顺序无关；
  不同逻辑块可完全并行。
- **确定性仲裁**：同一逻辑块并发写入时，以
  `arbitrationKey = writerID + "\x00" + sha256(整块数据)` 的字典序**较大者**胜出。
  胜出者的数据进入新检查点；落败者得到 `ErrPlacementConflict`，其副本被回滚。
  相同 `(writerID, data)` 的重复写入幂等成功且不产生新检查点。
- **快照可见性**：`head` 指向当前检查点且整体不可变，每次提交整体替换。读者只能看到
  完整的旧快照或完整的新快照，读不到半条带；读路径按副本顺序做故障转移，并在组装后
  再次校验整块 sha256，任何拼接/悬空结果都以校验错误暴露。

## 4. 检查点、恢复与空间回收

### 检查点

- 检查点是一个 JSON 清单：逻辑块 → 每个 chunk 的副本列表（后端、文件名、摘要、长度），
  通过临时文件 + fsync + 原子 rename + 目录 fsync 落盘，崩溃后不会出现半个清单。

### 恢复语义（`Open` 时自动执行，也可手动 `Recover()`）

1. 按 seq **从新到旧**逐个验证检查点：每个 chunk 至少有一个副本可读且 sha256 正确；
2. 发现损坏副本：将文件改名隔离为 `*.dat.quarantine-xxxx`（不再可见，但保留供排查），
   并把该副本引用从检查点中剔除、原地重写清单（只要还有至少一个好副本，检查点仍完好）；
3. 若最新检查点存在“所有副本均损坏/不可读”的 chunk，则**回退到上一个完好检查点**；
4. 重复恢复幂等：隔离与引用修复已生效后再次执行，结论不变、数据不重复不丢失；
5. 若没有任何完好检查点，返回 `ErrNoCheckpoint`，不会对外暴露损坏状态。

`RecoveryReport` 记录：依次检查过的 seq、最终选中 seq、发现的损坏副本、已隔离文件与
判定理由（测试会打印）。

### 空间回收（`GC()`）

- 只保留最新的 `KeepCheckpoints` 个**完好**检查点（损坏检查点与超额旧检查点删除）；
- 计算存活检查点引用集合 `(backend, file)`，删除：
  - 不被引用的块文件（旧版本、失败尝试残留、悬空副本）；
  - 写入崩溃残留的 `.tmp.*` 临时文件；
  - 恢复隔离的 `*.quarantine-*` 文件；
- 回收后**磁盘块文件集合与存活检查点引用集合完全一致**，
  `GCReport.LiveBytes` 等于现存块文件总字节，且 GC 幂等。

## 5. 故障注入能力（测试用）

`DiskBackend` 支持在运行时独立切换：

- `SetUnavailable(true)`：后端整体不可用（读写均失败）；
- `SetFailWrites(true)`：所有写入失败（模拟写入中途失败，临时文件不会转正）；
- `SetFailReads(true)`：所有读取失败；
- `CorruptBlock(id)`：翻转数据区一个字节，模拟静默读损坏，用于验证校验与回退。

## 6. 验证方法

```bash
go test ./... -v -count=1      # 全部单测，日志含分片布局与判定依据
go test -race ./...            # 竞态检测
go test ./... -count=5         # 多轮重复，验证确定性可复现
```

测试覆盖：

| 测试 | 覆盖点 |
|---|---|
| `TestBasicStripeWriteRead` | 确定性分片布局、副本分散、重组正确 |
| `TestPartialBackendFailure` | 健康后端不足的可区分失败；写入中途失败时不前进检查点、无半条带、无悬空副本 |
| `TestConcurrentWriteDeterminism` | 8 写者并发同块 → 固定胜者；200 次并发读看不到半成品 |
| `TestCheckpointRecovery` | 重启恢复、坏副本发现与隔离、重复恢复幂等、回退上一完好检查点 |
| `TestGarbageCollection` | 旧版本/悬空/临时/隔离文件回收，磁盘集合 == 存活引用集合，占用字节一致，GC 幂等 |
| `TestInterruptedConcurrentWrites` | 后端反复故障 + 多块多版本并发写入后“重启”恢复，每块恰好一个确定性胜出版本 |
| `TestReplicaFailover` | 副本故障透明转移；全副本不可用时给出可解释错误 |

## 7. 使用示例

```go
s, err := store.Open(store.Options{
    RootDir:   "./data",
    Backends:  []string{"disk0", "disk1", "disk2", "disk3"},
    ChunkSize: 4096,
    Replicas:  2,
})
if err != nil { log.Fatal(err) }
defer s.Close()

if err := s.Write("writer-1", 42, payload); err != nil {
    // err 可 errors.Is 区分原因，*store.WriteError 含阶段与失败后端
}
data, err := s.Read(42)

report, _ := s.Recover()   // 手动恢复/回退（Open 已自动执行一次）
gc, _     := s.GC()        // 空间回收
```
