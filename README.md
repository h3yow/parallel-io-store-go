# parallel-io-store-go

一套纯本地磁盘的**并行存储层**教学/验证实现：用若干本地目录模拟多个独立
存储后端，演示条带化写入、确定性多副本放置、并发一致性、检查点回退恢复
与垃圾回收，并让所有结论在部分故障与并发访问下**确定、可复现、可解释**。

不依赖任何外部服务或真实设备，仅使用标准库（Go ≥ 1.25）与本地文件系统。

## 1. 目录与文件结构

| 文件 | 职责 |
| --- | --- |
| `backend.go` | `DiskBackend`：本地目录模拟后端，原子写（临时文件+fsync+rename）、故障注入（写失败 / 下线 / 静默损坏）、容量预算与放置权重 |
| `placement.go` | 确定性分片与副本放置规则（加权环）、路径规则 |
| `store.go` | 存储层主逻辑：切块、条带化、并行副本写、原子提交、快照可见性、读重建、容量预检与占用统计、读取自愈、在线加后端 |
| `wal.go` | 只追加提交日志（带 CRC 的帧、幂等折叠、撕裂尾帧压缩、重定位记录） |
| `checkpoint.go` | 检查点清单的原子发布、逐分片验证、损坏回退 |
| `rebalance.go` | 在线扩容后的确定性重平衡（搬迁、校验、可中断重试） |
| `heal.go` | 副本修复原语与全量巡检（Scrub） |
| `gc.go` | 以存活提交集合为根集的空间回收与占用统计 |
| `errors.go` | 可区分的失败原因（哨兵错误 + 结构化错误） |
| `*_test.go` | 覆盖故障、并发、中断恢复、GC、容量、重平衡、自愈巡检的确定性测试 |

## 2. 分片与副本规则

1. 对象按 `BlockSize` 切成逻辑块，最后一块为短块。
2. 每个块按**字节轮转**切成 `S` 个分片：分片 `i` 持有块内第
   `i, i+S, i+2S, …` 个字节。读时按同一规则逆交织重建。
3. 每个分片有 `R` 个副本，必须落在 `R` 个**互不相同**的后端上。
4. 后端按 ID 排序构成健康环；每个后端带一个相对权重 `weight`
   （默认 1，`SetWeight` 调整）。先把环展开成加权环——按 ID 升序、
   每个后端重复 `weight` 次——分片 `s` 的第 `r` 个副本位置：

   ```
   weightedRing[(FNV-1a(objectID, block, s) + r) mod W]
   ```

   若槽位对应的后端在本分片内已被选中，则确定性地顺移到下一个未
   使用的后端。规则要点：

   - 位置只取决于 `(对象, 块, 分片, 副本, 后端配置, 当前健康环)`，
     与写入顺序、线程调度、已用空间无关，因此同一组输入不管写多少
     次、以什么并发顺序写，算出的位置都完全一样；
   - 权重只影响命中槽位的**分布倾向**（权重 w 的后端出现 w 个槽位），
     不引入任何随机性；所有权重为 1（默认）时展开环与原环一致，
     布局与旧版本逐位相同；
   - 放置只在**当前健康**后端中选择，后端下线不会被静默放入副本；
   - 健康后端数 `< R` 时，**在写入任何字节之前**直接失败
     （`ErrReplicaShortage` + `PlacementError` 判定依据），绝不少放副本。
5. 副本一旦随块提交，其后端位置会**冻结在提交记录**中。此后健康环成员
   变化，读取仍按提交时的固定位置寻址，不会发生副本漂移/错放。
6. 分片文件按版本隔离存放，新版本写失败不可能覆盖旧版本副本：

   ```
   shards/<obj>/b<block>/v<version>-<writer>/s<shard>/r<replica>.data
   ```

### 容量预算与占用统计

每个后端可设置容量预算 `SetCapacity(bytes)`（`<=0` 表示不限，默认不限）：

- **写前预检**：块写入在声明写键的同一临界区内，按确定性布局汇总各
  目标后端的分片字节需求，判定
  `已提交存活字节 + 在途预留 + 本次需求 <= 预算`。
  任一目标后端不满足则**整块在写任何字节之前失败**，错误为
  `ErrCapacityExhausted`（包在 `PlacementError` 中，含预算/已用/预留/
  需求的判定依据），与 `ErrReplicaShortage`（健康后端不足）可用
  `errors.Is` 明确区分；
- **已提交数据不可挤占**：容量判定的"已用"只来自当前生效的提交记录
  （与 GC 存活根集同源），已提交数据的空间永远不会被后续写入抢占，
  也不存在"写返回成功但某副本没地方放"——预检通过即预留，提交后
  预留转为已提交占用；
- **占用统计**：`CapacityReport()` 按后端返回
  `Budget / Weight / Used / Reserved / Remaining`，其中 `Used` 与
  GC 后磁盘上存活分片字节数严格相等（测试逐后端断言）。

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

## 6. 在线扩容与重平衡

### 加入后端（`AddBackend`）

- 运行中可随时加入新后端；**只加后端不做平衡时行为保持不变**：
  已有提交记录的位置不变、读取结果不变、恢复流程不变，新后端上
  不会出现任何旧数据文件；
- 新写入立即使用"含新后端的健康环"上的同一套确定性布局规则；
- 旧数据、旧 WAL、旧检查点全部向后兼容：旧日志没有重定位帧、
  记录 `Kind` 零值即普通提交，新代码直接读取。

### 重平衡（`Rebalance`）

- 目标布局 = 用当前健康环（含新后端）对每条已提交记录重算
  `BlockLayout`，与写入路径完全同一套确定性规则，因此无论执行
  多少次、被中断多少次，最终都收敛到**同一个布局**；
- 搬迁以提交记录为单位：`读出完好源副本（CRC 校验）→ 写入目标
  后端同一版本化路径 → 落盘后复读校验 → 向 WAL 追加 KindReloc
  重定位记录切换提交视图`。**新副本完整落盘并通过校验之前，旧
  位置始终是存活集**，因此重平衡全程读取不会失败，平衡前后同一
  批对象的可见内容与校验和完全一致；
- **中断与重试**：每条记录的"拷贝→校验→追加"序列都是幂等的。
  中途崩溃或被 `RebalanceOptions.Hook` 中止时，已生效的重定位
  保持有效，未生效的拷贝文件是垃圾（GC 回收）；重试只重做尚未
  生效的部分。重复执行已收敛的重平衡是空操作
  （`RecordsMoved == 0`），不产生多余副本、不丢块；
- **容量约束**：目标后端预算不足时该记录整体暂缓（不写任何字
  节），旧位置继续服务，报告与返回错误中含可区分的
  `ErrCapacityExhausted`；放宽预算后重试即可收敛；
- **空间回收**：重定位生效后旧位置文件不再被任何提交记录引用，
  由 `RunGC` 回收；回收后各后端占用与当前存活副本集合严格相等；
- 重平衡与并发读写兼容（共享读锁），与 GC / 检查点恢复 /
  `AddBackend` 互斥，保证旧位置文件不会在搬迁完成前被回收。

## 7. 读取自愈与巡检（Scrub）

### 读取自愈

- 读取时若首选副本损坏（CRC 不符）或缺失，但同一分片还有其它
  完好副本，则照常返回正确内容，并**顺手把完好字节补写回提交
  记录中固定的那个位置**（同一后端、同一版本化路径），副本数量
  随之恢复；
- 补写失败（如目标后端写故障）**不影响本次读取结果**，只记录为
  `HealEvent`（可通过 `HealEvents()` 观测），留待巡检或后续读取
  再处理；
- 自愈只写回已确定的位置，**绝不改变已生效的副本布局**，也不会
  改变同一时刻的读取结果（补写字节与提交记录中的分片 CRC 一致）。

### 巡检（`Scrub`）

- 对当前生效提交视图的全部副本做一遍健康检查，逐副本报告
  `ok / corrupt / missing / backend-unavailable` 及所在后端与路径；
- 自动修复能修好的部分：从同分片的完好副本取字节，写回坏/缺失
  副本的固定位置并复读校验；
- 返回可解释统计：检查了多少记录与副本、各类状态计数、修了多少、
  修复失败多少、还剩多少分片处于**降级**（已无任何完好副本、无法
  修复，读取会报 `ErrBlockDegraded`）；
- 巡检幂等：第二轮对同一健康状态重复执行，`Repaired` 为 0，
  降级计数稳定。

## 8. 失败原因速查

| 错误 | 含义 |
| --- | --- |
| `ErrReplicaShortage` | 健康后端数不足副本因子，放置前失败 |
| `ErrCapacityExhausted` | 目标后端容量预算放不下新副本，写任何字节前失败 |
| `ErrBackendUnavailable` | 目标后端被注入下线故障 |
| `ErrShardWriteFailed`（`*ShardWriteError`） | 某个分片副本写中途失败 |
| `ErrBlockIncomplete` | 块的部分副本未落盘，块未提交、不可见 |
| `ErrBlockDegraded` | 块已提交，但某分片所有副本均不可读/CRC 不符 |
| `ErrConflict` | 同 `(version,writer)` 提交了不同内容 |
| `ErrCheckpointCorrupt` / `ErrNoCheckpoint` | 清单损坏 / 无完好检查点可回退 |
| `ErrObjectNotFound` | 对象不存在任何完整快照 |

## 9. 验证方法

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
  占用与存活数据一致、GC 幂等；
- `TestCapacityExhaustedFailsBeforeAnyByte` /
  `TestFailureReasonsDistinguishable`：容量不足时不落任何字节、
  失败原因与后端不足可区分、已提交数据不被挤占；
- `TestCapacityReportMatchesLiveData`：各后端 预算/已用/剩余 与
  GC 后存活数据严格一致；
- `TestWeightInfluencesDistributionDeterministically`：权重只影响
  分布倾向，同输入多次独立运行布局逐位相同；
- `TestAddBackendWithoutRebalance`：只加后端不平衡时布局/读取/GC
  行为完全不变；
- `TestRebalanceConcurrentReadWriteConsistency`：并发读写下在线
  扩容与重平衡，读取不失败、内容校验和前后一致、布局收敛、旧位置
  被 GC 回收后占用 == 存活集合；
- `TestRebalanceInterruptRetryIdempotent` /
  `TestRebalanceSurvivesRestart`：重平衡中断后重试、跨重启续跑、
  重复执行幂等收敛；
- `TestRebalanceRespectsCapacity`：目标容量不足时记录整体暂缓、
  旧位置继续服务、放宽后收敛；
- `TestReadSelfHealsCorruptAndMissing` /
  `TestHealFailureDoesNotAffectRead` / `TestSelfHealRestoresFileBytes`：
  读取自愈补写固定位置、补写失败不影响读取、补写字节与提交记录
  CRC 一致、布局不变；
- `TestScrubReportAndDegraded`：巡检分类统计、自动修复、降级分片
  报告与幂等复扫。

测试日志使用 `t.Log` 打印**分片布局表**（块/分片/副本/后端/路径/CRC）、
放置规则与每一步"判定依据"，可用 `-v` 查看。例：

```
object=o S=4 R=2 blocks=2 blockSize=16 visible=v8/writer-7
placement rule: shard s replicas at weightedRing[(FNV1a(obj,b,s)+r) mod W] ...
 block 0: size=16 crc=...
   shard=0 replica=0 backend=d path=shards/o/b0/v8-writer-7/s0/r0.data shardCRC=...
```

### 兼容范围与验证方法

- **旧数据/元数据**：旧版本写入的 WAL（无 `Kind` 字段、无重定位帧）、
  检查点清单、分片文件均可被当前版本直接打开与恢复；默认权重（全 1）
  下布局规则与旧版逐位一致；
- **验证方法**：用旧版本二进制写入数据 → 用当前版本 `Open` 重放 →
  读取与校验和不变 → `Restore`/`RunGC` 行为不变
  （`TestRebalanceSurvivesRestart` 覆盖了"重定位记录跨重启重放"
  这一最复杂的兼容路径）。

## 10. 范围与限制

- 这是单机本地目录实现，重点是**语义的确定性与可解释性**，不模拟网络
  分区，也不做纠删码（采用多副本而非 RS 编码）；
- 元数据（WAL/检查点）放在单一指定后端目录中，原子性由本地
  rename/fsync 提供；
- 容量预检按"已提交存活 + 在途预留"判定，重平衡期间旧位置副本在新
  位置生效前仍计入占用（保守方向，不会超卖）；
- 块写入内部并行，对象级跨块按块号顺序提交，以保证失败时的快照边界
  简单清晰。
