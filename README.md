# reorg-aware-chain-indexer-go

可复用的链上数据索引组件。使用本地模拟节点 RPC 提供区块数据，在**乱序、重复投递、链回滚（重组）、节点故障**等异常情况下，索引结果都能收敛到同一份正确结论。

## 结构

```
internal/chain     区块类型、可区分拒绝原因、已确认区块存储（Store / MemoryStore）
internal/rpc       节点抽象、MockNode（可注入延迟/挂起/重组视图）、故障切换客户端 Client
internal/indexer   Sequencer（乱序归一）+ Indexer（确认深度、重组撤销与重放）
```

## 确认深度策略

- 区块经 `Sequencer` 按高度归一后进入 `pending`，**不会立即落库**。
- 只有当 `latestHeight - height >= confirmDepth` 时，`Finalize(latestHeight)` 才把该高度落库。
- 未达确认深度的区块保留在 pending 中等待，日志会打印 `unconfirmed` 等待原因。
- 落库是幂等的：同高度同 hash 已落库时直接跳过，链抖动不会重复计入。

## 乱序 / 重复 / 缺口的可区分原因

`Ingest` 返回 `*chain.RejectError`，`Reason` 可区分：

| Reason | 含义 |
|---|---|
| `gap` | 高度缺口，区块已缓存等待缺失高度，**不会静默跳过** |
| `duplicate` | 重复投递，已见过该 hash |
| `orphan` | 父哈希与已知链不吻合，已缓存等待父块或重组点 |
| `conflict` | 已消费高度出现未知 hash，触发重组判定 |
| `unconfirmed` | 未达确认深度（Finalize 日志中体现） |

## 重组处理规则

1. 新区块落在已消费高度且 hash 未知 → 判定是否构成重组：检查其父哈希是否与已知链（pending 优先，其次已落库）在 `height-1` 处吻合。
2. 吻合 → 定位分叉点，截断 pending、回滚已落库区块（`Store.Rollback`），重置定序器，沿新分支重放；被撤销的旧链块重新入队，与新链不一致的以可区分原因丢弃。
3. 不吻合 → 标记 `orphan` 并缓存；父块乱序到达或重组点出现时自动重放（`drainOrphans`）。
4. 落库时发现同高度 hash 冲突（兜底路径）→ 先回滚再落库。
5. 日志打印回滚判定依据：分叉点高度、吻合的父哈希、撤销的 pending/已落库数量。

## 多节点故障切换与并发拉取

- `rpc.Client` 对每次问询并发请求全部节点（带超时），挂起/超时的节点自动被跳过（故障切换）。
- 一致性采用**响应数严格多数决**：得票超过实际响应数一半才采纳；矛盾且无多数（如 1v1）返回错误而非随机选择；全部失败返回 `ErrAllNodesFailed`。
- `FetchRange` 并发拉取高度区间（并发度受限，内存有界），结果写入定长切片的唯一下标，**保证按高度升序、无数据竞争**（`-race` 验证）。
- 定序器缓冲、pending、孤儿池均随确认推进而回收，长时间运行内存占用稳定（见 `TestLongRunStable`）。

## 本地验证

```bash
# 全部测试（含 race 检测）
CGO_ENABLED=1 go test ./... -count=1 -race

# 查看高度流水与回滚判定日志
go test ./internal/indexer/ -run TestReorgConverges -v

# 场景覆盖
#   TestOutOfOrderAndDuplicate      乱序 + 重复 + 缺口原因
#   TestConfirmationDepth           确认深度
#   TestReorgConverges              重组收敛
#   TestReorgIdempotentReplay       抖动重复投递不重复计入
#   TestOrphanRejected              孤儿块可区分拒绝
#   TestFailoverOnHang              节点挂起故障切换
#   TestConflictingNodesMajority    矛盾数据多数决
#   TestFetchRangeOrderedAndConcurrent 并发拉取不乱序
#   TestEndToEndReorgConvergence    端到端：拉取→落库→重组→收敛
#   TestLongRunStable               长时运行稳定性
```
