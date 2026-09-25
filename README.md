# parallel-io-store-go

一套纯本地磁盘的**并行存储层**教学/验证实现：用若干本地目录模拟多个独立
存储后端，演示条带化写入、确定性多副本放置、并发一致性、检查点回退恢复
与垃圾回收，并让所有结论在部分故障与并发访问下**确定、可复现、可解释**。

不依赖任何外部服务或真实设备，仅使用标准库（Go ≥ 1.25）与本地文件系统。

## 1. 目录与文件结构

| 文件 | 职责 |
| --- | --- |
| `backend.go` | `DiskBackend`：本地目录模拟后端，原子写（临时文件+fsync+rename）与故障注入（写失败 / 下线 / 静默损坏） |
| `placement.go` | 确定性分片与副本放置规则、路径规则 |
| `store.go` | 存储层主逻辑：切块、条带化、并行副本写、原子提交、快照可见性、读重建 |
| `wal.go` | 只追加提交日志（带 CRC 的帧、幂等折叠、撕裂尾帧压缩） |
| `checkpoint.go` | 检查点清单的原子发布、逐分片验证、损坏回退 |
| `gc.go` | 以存活提交集合为根集的空间回收与占用统计 |
| `errors.go` | 可区分的失败原因（哨兵错误 + 结构化错误） |
| `*_test.go` | 覆盖故障、并发、中断恢复、GC 的确定性测试 |

## 2. 分片与副本规则

1. 对象按 `BlockSize` 切成逻辑块，最后一块为短块。
2. 每个块按**字节轮转**切成 `S` 个分片：分片 `i` 持有块内第
   `i, i+S, i+2S, …` 个字节。读时按同一规则逆交织重建。
3. 每个分片有 `R` 个副本，必须落在 `R` 个**互不相同**的后端上。
4. 后端按 ID 排序构成健康环 `ring[0..N-1]`；分片 `s` 的第 `r` 个副本位置：

   ```
   ring[(FNV-1a(objectID, block, s) + r) mod N]
   ```

   - 位置只取决于 `(对象, 块, 分片, 副本, 当前健康环)`，与写入顺序、线程
     调度无关，因此任何时刻重算都得到同一布局；
   - 放置只在**当前健康**后端中选择，后端下线不会被静默放入副本；
   - 健康后端数 `< R` 时，**在写入任何字节之前**直接失败
     （`ErrReplicaShortage` + `PlacementError` 判定依据），绝不少放副本。
5. 副本一旦随块提交，其后端位置会**冻结在提交记录**中。此后健康环成员
   变化，读取仍按提交时的固定位置寻址，不会发生副本漂移/错放。
6. 分片文件按版本隔离存放，新版本写失败不可能覆盖旧版本副本：

   ```
   shards/<obj>/b<block>/v<version>-<writer>/s<shard>/r<replica>.data
   ```

### 写提交协议（不丢块、不错放）

```
preflight(健康后端数 >= R)
  → 计算确定性布局
  → S*R 个副本并行写（每个副本都是临时文件+fsync+rename 的原子完整文件）
  → 任一副本失败：整块判定 ErrBlockIncomplete（聚合每个 *ShardWriteError），
    不追加提交记录；已落盘字节成为垃圾，等待 GC，绝不半提交
  → 全部 durable：在 WAL 追加一条提交记录（块 CRC + 每分片 CRC + 副本位置）
```

只有 WAL 中存在提交记录的块才可见，因此读取永远观察不到"只写了一半的
条带"或悬空副本。

## 3. 并发一致性语义

每次写入携带 `(Version, Writer)`：

- **快照原子性**：一个 `(Version, Writer)` 是对象的一个原子快照。
  新版本必须**所有块都已提交**才整体可见；写入中途失败时，新版本整体
  不可见，读取确定性地回退到上一个块齐全的版本（不会拼出跨版本的混合
  数据）。
- **确定性全序**：快照按 `(Version 升序, Writer 字典序)` 排序，读取取
  最大的完整快照。因此多个写入者并发覆盖同一对象时，最终结果只由它们的
  `(version, writer)` 决定，与 WAL 追加顺序、goroutine 调度无关，重复
  运行结论一致。
- **幂等与冲突可区分**：同 `(version, writer)` + 相同内容的重复提交幂等
  成功（崩溃重试安全）；相同键但不同内容返回 `ErrConflict`，绝不静默
  覆盖。
- 读路径对每个分片按副本号依次尝试，校验分片 CRC，再校验重建后的整块
  CRC；单副本损坏/所在后端下线会自动切换到其它健康副本。

## 4. WAL、检查点与恢复语义

### WAL（`meta/commit.wal`）

- 帧格式：`"PWAL" | len(4) | TLV payload | CRC32C(payload)(4)`。
- 重放时按 Key `(object, block, version, writer)` 折叠重复记录：重复
  恢复**不重不丢**；同 Key 载荷冲突直接报错，不接受伪造提交。
- 撕裂/CRC 错误的尾帧被忽略（未提交数据不可见）；打开日志时若发现坏尾
  帧，会**压缩重写**为只含完好帧的规范文件，保证后续新提交不会被坏尾
  永久截断。

### 检查点（`meta/checkpoints/cp-<seq>.json` + `meta/CURRENT`）

- 检查点是当时全部已提交记录的不可变 JSON 清单，临时文件 + rename
  原子发布，序号单调；CURRENT 原子指向最新检查点。
- 恢复（`Restore`）从最新检查点开始**从新到旧**验证：清单引用的每个
  分片必须**至少有一个副本字节存在且 CRC 正确**；
  - 最新检查点损坏（如分片被静默篡改、副本丢失）→ 记录每个坏分片位置与
    原因，**回退到上一个完好检查点**：把 WAL 原子压缩为该检查点的记录
    集（回退决定在重启后依然成立，之后的提交视为被回退、其分片由 GC
    回收），并将内存提交视图整体重置到该快照；
  - 全部检查点都损坏 → 返回 `ErrNoCheckpoint`，`CheckpointResult.Tried`
    保留完整探测链；
  - 恢复可任意重复执行，选中序号与存活数据幂等不变。

## 5. 空间回收（GC）

- 存活根集 = 当前生效视图中所有提交记录 `Locs` 引用的精确
  `(backend, path)` 对；
- 枚举各后端 `shards/` 下所有 `.data`，不在根集中即删除：包括写入中途
  失败的悬空副本、被更新快照取代的旧版本分片、损坏检查点时代的孤儿；
- GC 幂等：第二次运行删除数必为 0；
- `SpaceUsage()` 统计各后端占用；测试断言 GC 后占用字节数与存活集合
  逐文件之和严格相等。

## 6. 失败原因速查

| 错误 | 含义 |
| --- | --- |
| `ErrReplicaShortage` | 健康后端数不足副本因子，放置前失败 |
| `ErrBackendUnavailable` | 目标后端被注入下线故障 |
| `ErrShardWriteFailed`（`*ShardWriteError`） | 某个分片副本写中途失败 |
| `ErrBlockIncomplete` | 块的部分副本未落盘，块未提交、不可见 |
| `ErrBlockDegraded` | 块已提交，但某分片所有副本均不可读/CRC 不符 |
| `ErrConflict` | 同 `(version,writer)` 提交了不同内容 |
| `ErrCheckpointCorrupt` / `ErrNoCheckpoint` | 清单损坏 / 无完好检查点可回退 |
| `ErrObjectNotFound` | 对象不存在任何完整快照 |

## 7. 验证方法

```bash
go test ./... -race -count=10 -v
```

测试覆盖：

- `TestReplicaShortage`：健康后端不足时放置前失败，且无任何分片落盘；
- `TestMidWriteFailure`：写中途失败块不可见、旧版本完整可读、幂等重试成功；
- `TestWriteConflict`：同版本同写入者不同内容确定报冲突；
- `TestCorruptReplicaDetected`：单副本静默损坏自动切换，全副本损坏报降级；
- `TestOfflineBackendAfterCommit`：提交后后端下线的读取降级路径；
- `TestConcurrentWritersDeterministic` /
  `TestSameVersionConcurrentDifferentWriters`：多写入者并发，含并发读者
  巡检，最终胜者由全序确定，`-count=N` 重复结论一致；
- `TestInterruptedWriteAcrossRestart`：写入中途"崩溃"后重新 Open，
  旧版本完好、幂等续写、GC 回收残留；
- `TestWALTornTailIgnored`：撕裂 WAL 尾帧不伪造提交、打开时压缩、
  压缩后新提交在再次重启后仍在；
- `TestCheckpointAndReopen` / `TestRestoreFallsBackToLastIntact` /
  `TestAllCheckpointsCorrupt`：检查点发布、重启重放、损坏发现与逐级回退；
- `TestGCReclaimsGarbageAndMatchesLiveData`：悬空/旧版本分片回收、
  占用与存活数据一致、GC 幂等。

测试日志使用 `t.Log` 打印**分片布局表**（块/分片/副本/后端/路径/CRC）、
放置规则与每一步"判定依据"，可用 `-v` 查看。例：

```
object=o S=4 R=2 blocks=2 blockSize=16 visible=v8/writer-7
placement rule: shard s replicas at ring[(FNV1a(obj,b,s)+r) mod N] ...
 block 0: size=16 crc=...
   shard=0 replica=0 backend=d path=shards/o/b0/v8-writer-7/s0/r0.data shardCRC=...
```

## 8. 范围与限制

- 这是单机本地目录实现，重点是**语义的确定性与可解释性**，不模拟网络
  分区，也不做纠删码（采用多副本而非 RS 编码）；
- 元数据（WAL/检查点）放在单一指定后端目录中，原子性由本地
  rename/fsync 提供；
- 块写入内部并行，对象级跨块按块号顺序提交，以保证失败时的快照边界
  简单清晰。
