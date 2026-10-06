# parallel-io-store-go

一套纯本地磁盘的**并行存储层**教学/验证实现：用若干本地目录模拟多个独立
存储后端，演示条带化写入、确定性多副本放置（容量预算 + 相对权重）、
并发一致性、检查点回退恢复、垃圾回收、在线扩容重平衡与读取自愈/巡检，
并让所有结论在部分故障与并发访问下**确定、可复现、可解释**。

不依赖任何外部服务或真实设备，仅使用标准库（Go ≥ 1.25）与本地文件系统。

## 1. 目录与文件结构

| 文件 | 职责 |
| --- | --- |
| `backend.go` | `DiskBackend`：本地目录模拟后端，原子写（临时文件+fsync+rename）与故障注入（写失败 / 下线 / 静默损坏） |
| `placement.go` | 确定性分片与副本放置规则（加权环选位 + 选后容量判定）、路径规则 |
| `store.go` | 存储层主逻辑：切块、条带化、并行副本写、原子提交、快照可见性、读重建、读自愈钩子 |
| `capacity.go` | 后端容量预算与相对权重、写入前容量预检与预留、重平衡容量增量判定、按后端的占用统计 |
| `rebalance.go` | 在线加入后端、确定性重平衡（复制→校验→原子切换布局） |
| `heal.go` | 读取自愈（补写既定位置）与全量巡检修复（`Scrub`） |
| `wal.go` | 只追加提交日志（带 CRC 的帧、幂等折叠、撕裂尾帧压缩、位置取代记录） |
| `checkpoint.go` | 检查点清单的原子发布、逐分片验证、损坏回退 |
| `gc.go` | 以存活提交集合为根集的空间回收与占用统计 |
| `errors.go` | 可区分的失败原因（哨兵错误 + 结构化错误） |
| `*_test.go` | 覆盖故障、并发、中断恢复、GC、容量、重平衡、自愈巡检的确定性测试 |

## 2. 分片与副本规则

1. 对象按 `BlockSize` 切成逻辑块，最后一块为短块。
2. 每个块按**字节轮转**切成 `S` 个分片：分片 `i` 持有块内第
   `i, i+S, i+2S, …` 个字节。读时按同一规则逆交织重建。
3. 每个分片有 `R` 个副本，必须落在 `R` 个**互不相同**的后端上。
4. 后端按 ID 排序构成健康环，再按相对权重经**平滑加权轮转**展开为
   加权环（权重 `w` 的后端在环上出现 `w` 次且均匀散开；全部权重为 1
   时加权环与原环逐元素相同）。分片 `s` 的副本选择：从加权环位置
   `FNV-1a(objectID, block, s) mod L` 起顺序扫描，依次挑出 `R` 个
   **互不相同**的后端（全权重 1 时等价于旧规则 `ring[(h + r) mod N]`，
   旧部署布局逐位不变）。

   - 位置只取决于 `(对象, 块, 分片, 副本数, 当前健康环及其权重配置)`，
     与写入顺序、线程调度、**各后端当前剩余空间**都无关，因此同一组
     输入无论写多少次、以什么并发顺序写、各后端已被占掉多少空间，
     算出的位置完全一致；权重只改变分布倾向，**不改变已确定的落点**，
     也不引入任何随机性；
   - **容量只用于判定，不参与选位**：位置固定之后，把本块落在每个
     后端上的字节数与该后端预算内剩余比较——放得下才写，放不下整块
     失败，**绝不因为某个后端当下剩余空间少就把副本挪到别的后端**；
   - 放置只在**当前健康**后端中选择，后端下线不会被静默放入副本；
   - 健康后端数 `< R` 时，**在写入任何字节之前**直接失败
     （`ErrReplicaShortage` + `PlacementError` 判定依据），绝不少放副本；
   - 后端数量足够但这组已确定的位置在预算内放不下时，同样在写字节
     之前失败（`ErrCapacityExhausted`，与前者可区分，见第 6 节）。
5. 副本一旦随块提交，其后端位置会**冻结在提交记录**中。此后健康环成员
   变化，读取仍按提交时的固定位置寻址，不会发生副本漂移/错放。
6. 分片文件按版本隔离存放，新版本写失败不可能覆盖旧版本副本：

   ```
   shards/<obj>/b<block>/v<version>-<writer>/s<shard>/r<replica>.data
   ```

### 写提交协议（不丢块、不错放）

```
preflight(健康后端数 >= R；容量预检 + 预留，与冲突检查同一把锁)
  → 计算确定性布局（加权环选位，位置与当前占用无关）
  → 对这组已确定的位置做容量判定：放不下则整体失败
  → S*R 个副本并行写（每个副本都是临时文件+fsync+rename 的原子完整文件）
  → 任一副本失败：整块判定 ErrBlockIncomplete（聚合每个 *ShardWriteError），
    不追加提交记录；已落盘字节成为垃圾，等待 GC，绝不半提交
  → 全部 durable：在 WAL 追加一条提交记录（块 CRC + 每分片 CRC + 副本位置）
```

容量预检在"声明写入键"的同一把锁内完成：先按当前生效提交集合算出各
后端已用字节，加上在途写入的预留，再检查本次布局是否放得下；放不下
则整体失败（`ErrCapacityExhausted`），**尚未写任何字节**。预留在提交
或失败时释放。因此已提交数据占用的空间绝不会被后续写入或重平衡挤占，
也不会出现"写返回成功但某个后端已用空间超过其预算"。

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

## 6. 容量预算与相对权重

- 每个后端可用 `SetCapacity(budgetBytes, weight)` 配置容量预算
  （字节，`0` = 不限）与相对权重（`<1` 视为 `1`）；
- **权重只改变分布倾向，不改变已确定的落点**：权重通过平滑加权轮转
  决定副本在环上的分布倾向，位置本身是
  `(对象, 块, 分片, 副本数, 后端集合+权重)` 的纯函数——只要这组输入
  不变，无论各后端当前被占掉多少空间、写多少次、以什么并发顺序写，
  落点完全一致；
- **容量只用于判定**：对已确定的位置检查"预算内剩余是否放得下"
  （已用 + 在途预留 + 本次需要 ≤ 预算）。放得下才写；放不下整体失败，
  不发生位置改换。判定与预留在同一把锁内完成，因此并发写入之间、
  写入与重平衡之间都不会重复花费同一份容量，**任何后端的已提交
  占用（Used）任何时候都不会超过预算**（重平衡为"同后端换位置"的
  块会短暂预支即将释放的旧位置，`Used+Reserved` 可能瞬时略超预算，
  差额以该块即将释放的旧位置字节为界，切换后即回到预算内）；
- 失败原因可区分：健康后端数不够 → `ErrReplicaShortage`；
  数量够但已确定的位置在预算内放不下 → `ErrCapacityExhausted`
  （都包在 `*PlacementError` 里，附判定依据）；两者都发生在落盘
  任何字节之前；
- `CapacityReport()` 按后端返回 `Budget / Used / Reserved /
  Remaining`：`Used` 从当前生效提交集合精确推导，与 GC 之后
  `SpaceUsage()` 的磁盘实测逐字节相等（测试断言）。

## 7. 在线扩容与重平衡

- `AddBackend(b)` 只把新后端加入健康环：**不迁移任何已有数据**，
  已有布局、已有元数据、恢复流程完全不变（向后兼容）；新写入
  可能落到新后端；
- `Rebalance()` 做一次确定性重平衡：按当前健康环 + 权重配置重算
  每个存活块的目标布局（目标位置是确定性的，与各后端当前剩余空间
  无关），对每个需要迁移的副本执行
  **复制到目标位置 → 回读校验 CRC → 全部就位后用 WAL 位置取代
  记录原子切换该块布局**；
  - 切换发生之前旧副本继续服务读取，切换之后新副本已验证完好，
    因此整个过程中**读取不会失败**，内容、校验和与平衡前一致；
  - **搬迁遵循与新写入一致的容量规则**：切换前先对目标位置做容量
    增量判定并预留（`已用 - 释放 + 新增 + 预留 ≤ 预算`，与新写入
    同一把锁、同一本账）。目标位置预算内放不下时**暂缓该块**
    （`RebalanceReport.DeferredBlocks` 计数）：旧副本继续保留、
    正常可读，一个字节都不硬塞，等条件允许（预算调高 / GC 回收
    垃圾）后的下一轮再搬——绝不为了把重平衡跑完而把某个后端写爆；
  - 可中断、可重试、可重复触发：中断时未切换的块保持旧布局，
    半成品复制文件不在存活集合内（GC 回收）；重试从当前状态
    继续，已收敛的块幂等跳过、被暂缓的块每轮重试且暂缓集合确定；
    最终收敛到同一个布局，不产生多余副本、不丢块；
  - 被取代的旧位置副本在切换后成为垃圾，由 `RunGC` 回收，
    回收后各后端占用等于当前存活副本集合；
  - 重平衡与普通读写并发（`opMu` 读锁），与 GC/Restore 互斥，
    保证复制期间旧副本不被回收、提交视图不被整体替换；
    多趟重平衡之间由 `rebalMu` 串行化；
  - 重平衡与并发写入叠加时，任何后端的已提交占用（Used）都不会
    超过预算（测试以监视器协程全程断言）；
- WAL 中的"位置取代"记录（同 Key、仅 `Locs` 不同）在重放时
  后来者生效，因此重启后重平衡结论依然成立；旧版本 WAL（无
  此类记录）重放行为不变。

## 8. 读取自愈与巡检

- **读取自愈**：读路径对首选副本读失败/CRC 不符时，自动切换到
  同分片其它完好副本返回正确内容，并顺手把完好字节**补写回该
  副本在提交记录中的既定位置**（不改布局、不影响本次读取）；
  补写失败（如后端不可用）只记录一条 `RepairEvent`
  （`RepairEvents()` 可查），留待后续处理；
- **巡检** `Scrub()`：对当前所有存活对象逐副本检查
  （`ok / missing / corrupt / unavailable`），凡同分片还有完好
  副本的就地修复并回读校验，返回 `ScrubReport`：检查了多少对象
  与副本、多少本来就完好、修了多少、修复失败多少、还有多少分片
  处于降级（整分片无完好副本，如实上报不静默）；
- 自愈与巡检都只写回已生效布局中的既定路径，绝不改变副本布局，
  也不会让同一时刻的读取结果发生变化；两者幂等，可任意重复执行。

## 9. 失败原因速查

| 错误 | 含义 |
| --- | --- |
| `ErrReplicaShortage` | 健康后端数不足副本因子，放置前失败 |
| `ErrCapacityExhausted` | 后端数量足够但容量预算放不下，写字节前失败 |
| `ErrBackendUnavailable` | 目标后端被注入下线故障 |
| `ErrShardWriteFailed`（`*ShardWriteError`） | 某个分片副本写中途失败 |
| `ErrBlockIncomplete` | 块的部分副本未落盘，块未提交、不可见 |
| `ErrBlockDegraded` | 块已提交，但某分片所有副本均不可读/CRC 不符 |
| `ErrConflict` | 同 `(version,writer)` 提交了不同内容 |
| `ErrCheckpointCorrupt` / `ErrNoCheckpoint` | 清单损坏 / 无完好检查点可回退 |
| `ErrObjectNotFound` | 对象不存在任何完整快照 |

## 10. 验证方法

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
- `TestCapacityExhaustedNoBytesWritten` /
  `TestShortageVsCapacityDistinguishable`：容量不足整体失败且不落
  任何字节，失败原因与"后端不足"可区分；
- `TestPlacementIndependentOfOccupancy` /
  `TestLayoutPureFunctionOfConfig`：同一输入在空盘/不均匀占用/
  预算紧张（但够）下落点逐位一致；确定位置放不下时整体失败而
  不是改换位置；规划器层面不同剩余空间视图算出同一布局；
- `TestCapacityReportMatchesLiveData` /
  `TestCommittedDataNotEvictedByLaterWrites`：占用统计与存活数据
  严格一致，已提交数据不被后续写入挤占；
- `TestWeightInfluencesPlacementDeterministically`：权重只影响分布
  倾向且布局完全确定；
- `TestAddBackendWithoutRebalanceKeepsLayout` /
  `TestRebalanceConvergesAndIdempotent` /
  `TestRebalanceConcurrentReadWrite` /
  `TestRebalanceInterruptRetryIdempotent` /
  `TestConcurrentWriteAndRebalanceRespectBudgets` /
  `TestRebalanceDefersWhenCapacityInsufficient`：只加后端不改旧布局；
  重平衡前后内容/校验和一致、并发读写下读取不失败、中断重试与
  重复执行幂等收敛、旧位置由 GC 回收且占用等于存活集合；
  带预算时写入与重平衡并发，全程无任何后端超预算；
  目标预算不足时暂缓该块、保留旧副本，预算放宽后收敛且幂等；
- `TestReadSelfHealsCorruptReplica` /
  `TestReadSelfHealFailureDoesNotAffectRead` /
  `TestScrubReportsAndRepairs` /
  `TestScrubDegradedWhenNoIntactReplica`：读取自愈补写既定位置、
  补写失败不影响读取、巡检报告并修复、整分片损坏如实报降级。

测试日志使用 `t.Log` 打印**分片布局表**（块/分片/副本/后端/路径/CRC）、
放置规则与每一步"判定依据"，可用 `-v` 查看。例：

```
object=o S=4 R=2 blocks=2 blockSize=16 visible=v8/writer-7
placement rule: shard s replicas = first R distinct backends on weighted ring (smooth weighted round-robin by configured weights) from position FNV1a(obj,b,s) mod L; positions depend only on (object, block, shard, R, backend set+weights) — capacity is checked after positions are fixed and never relocates them; locations frozen in commit record
 block 0: size=16 crc=...
   shard=0 replica=0 backend=d path=shards/o/b0/v8-writer-7/s0/r0.data shardCRC=...
```

## 11. 范围与限制

- 这是单机本地目录实现，重点是**语义的确定性与可解释性**，不模拟网络
  分区，也不做纠删码（采用多副本而非 RS 编码）；
- 元数据（WAL/检查点）放在单一指定后端目录中，原子性由本地
  rename/fsync 提供；
- 块写入内部并行，对象级跨块按块号顺序提交，以保证失败时的快照边界
  简单清晰。
