# reorg-aware-chain-indexer-go

一个可复用的**链上数据索引组件**：用本地模拟的节点 RPC 提供区块数据，在
**乱序、重复投递、链回滚（重组）、多节点故障**等异常情况下，索引结果都能
收敛到同一份正确结论。

## 目标与不变量

- 区块按高度**顺序归一**后才落库；高度缺口不会被静默跳过，而是以
  可区分的原因（`gap_ahead`）进入缓冲等待。
- 只有达到**确认深度**的区块才正式写入 `Store`。
- 已索引区块在链回滚后会被**逐块撤销**并重放到新的规范链，派生计数
  （事件计数）随撤销/重放增减，最终收敛，绝不重复计入或错误丢弃。
- 多节点并发取数 + **多数派哈希仲裁**：节点超时/宕机自动切换，
  返回矛盾数据的节点被隔离；取数并发、落库串行，无乱序落库与数据竞争。
- 长时间运行内存有界：乱序缓冲有硬上限，并发在途量由 `MaxInFlight` 限制。

## 包结构

| 文件 | 职责 |
| --- | --- |
| `types.go` / `errors.go` | 区块/事件/流水类型，以及可被 `errors.Is` 区分的哨兵错误与原因枚举 |
| `store.go` | 已确认区块与派生计数的内存存储；按高度连续落库、从 tip 向下撤销，含父哈希链式校验 |
| `indexer.go` | 总装：缓冲、缺口/确认判定、回滚重放、后台有界拉取循环（所有状态变更串行） |
| `reorg.go` | 沿父哈希回溯定位分叉点，产出人类可读的“判定依据”，执行回滚 |
| `fetcher.go` | 多节点同高度并发取数、多数派仲裁、矛盾节点隔离、故障切换、有界区间并发拉取 |
| `logs.go` | 线程安全的高度流水收集器，供测试打印与断言 |
| `mock/chain.go`、`mock/node.go` | 本地模拟节点 RPC：可重组区块图、宕机/延迟、周期性抖动、拜占庭矛盾数据 |

## 确认深度策略

- 配置项 `Config.Confirmations = k`。
- 高度 `h` 的区块在已知链头 `tip` 满足 `tip - h >= k` 时才允许 `PutConfirmed`，
  即它“身后”至少有 `k` 个区块。
- 未达到深度的连续区块停留在内存缓冲中，仅在 `tip` 推进或新区块到达时
  尝试 `advance()`；缓冲区块**不参与**派生计数。
- 取数批量大小与 worker 数均为 `MaxInFlight`，因此在途区块数量有界。

## 重组（链回滚）处理规则

1. **发现冲突**：同一高度出现与已确认链 / 缓冲中区块不同的哈希。
2. **定位分叉点**：从冲突高度沿新区块的 `Parent` 指针逐格回溯，与已知链
   逐高度比对，第一个哈希一致的高度即共同祖先，其上方为分叉点 `fork`。
   - 若新链父区块尚未到达，则记录“临时分叉点（tentative fork）”，
     待更多区块到达后自动修正判定。
3. **回滚**：删除 `fork` 及以上属于旧链的未确认缓冲，并对存储从
   `tip` 到 `fork` 逐块 `Detach`，同步回退派生计数。
4. **重放**：新链区块按高度重新进入顺序归一流程，落库时记为 `attach`
   （区别于普通 `confirm`）。
5. **链式校验兜底**：当冲突区块直接落在下一个待处理高度、冲突点在其下方时
   （运行中后台拉取的典型场景），父哈希不匹配会触发“回退一格向下找分叉”，
   因此无需重启即可自动收敛；`Store` 的父哈希校验保证旧链残留不会错接到新链。
6. **抖动幂等**：重复投递相同区块返回 `ErrAlreadyCanonical` /
   `ErrAlreadyBuffered`，计数不翻倍；短暂全节点不可用只等待、不丢状态。

## 多节点故障切换与仲裁

- 同一高度并发请求所有健康节点，按区块**哈希计票**，多数派哈希为结论；
  平票时取哈希字典序最小者，保证结论确定。
- 返回少数派哈希的节点本轮被隔离（流水原因 `node_quarantine`），
  下一个轮询周期可重新加入多数派（恢复中的节点自动归队）。
- 超时/不可用节点被跳过并记 `failover`；全部不可用时返回
  `ErrNoHealthyNode`，本轮不落任何区块，下轮重试。
- 并发**只发生在取数阶段**；结果按高度排序后逐个 `Submit`，
  状态变更始终单线程串行（`stateMu`），`-race` 干净。

## 可区分的拒绝/等待原因

| 原因（`RejectReason`） | 含义 |
| --- | --- |
| `gap_ahead` | 前方存在高度缺口，区块已缓冲、等待补齐（也用于缓冲超上限拒绝） |
| `dup_canonical` | 区块已在规范链上确认（重复投递） |
| `dup_buffer` | 区块已在乱序缓冲中（重复投递） |
| `hash_conflict` | 同高度哈希冲突（触发重组判定） |
| `node_quarantine` | 节点返回与多数派矛盾的数据，被隔离 |
| `no_healthy_node` | 当前无任何可用节点 |

## 本地验证

```bash
# 编译与静态检查
go build ./...
go vet ./...

# 全部单测
go test ./...

# 并发安全（需要 cgo）
CGO_ENABLED=1 go test -race -count=1 ./...

# 查看带“高度流水 / 回滚判定依据”的详细日志
go test ./... -v
```

### 场景覆盖（`*_test.go`）

- `TestOutOfOrderAndGap`：乱序 + 缺口等待 + 顺序归一。
- `TestDuplicateDelivery`：已确认/缓冲中两种重复投递分别可区分。
- `TestConfirmationDepth`：确认深度前后的落库边界。
- `TestReorgConfirmedRollbackAndConverge`：已确认区块回滚、定位分叉、重放收敛。
- `TestReorgIdempotentReplay`：多轮乱序/重复（抖动）下计数精确收敛。
- `TestRuntimeReorgConvergence`：后台运行中切换到更长侧链，无需重启自动收敛。
- `TestFailoverOnUnavailable` / `TestAllNodesDownThenRecover`：
  主节点故障切换、全节点短暂宕机后恢复。
- `TestContradictingNodeQuarantined`：拜占庭节点被多数派隔离，结果一致。
- `TestBoundedBufferRejectsFarAhead`：超远乱序区块不撑爆缓冲。
- `TestLongRunBoundedMemory`：300 区块边生产边索引 + 周期抖动，
  缓冲峰值有界、无重复计数。

测试通过 `LogCollector` 打印形如下面的高度流水：

```
[reorg  ] h=1 hash=95b730 old=18b613 new=95b730 detail=hash conflict at h=1
         (old=18b613 new=95b730); common ancestor h=0 matches 526f2d, rollback from h=1
[detach ] h=2 hash=c2e7af detail=parent=18b613
[detach ] h=1 hash=18b613 detail=parent=526f2d
[attach ] h=1 hash=95b730 detail=replay onto new canonical chain
```

## 在自己的代码里使用

```go
store := indexer.NewStore()
collector := indexer.NewLogCollector()

ix := indexer.New(indexer.Config{
    Confirmations: 2,    // 2 个确认
    MaxInFlight:   4,    // 并发取数/批量上限
    StartHeight:   0,
}, store, []indexer.Client{node1, node2, node3}, collector.Logger())

go ix.Run(ctx)        // 后台有界拉取循环
defer ix.Stop()

// 也可以手动投递（允许乱序/重复）：
if err := ix.Submit(block); errors.Is(err, indexer.ErrGapAhead) {
    // 正常等待，缺口补齐后自动推进
}

store.Count("some-event-key") // 最终收敛的派生计数
```
