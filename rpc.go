package indexer

import "context"

// Client 是本地模拟节点 RPC 的最小接口。
// 实现方约定：
//   - 高度超过链头时返回包装了 ErrNotFound 的错误；
//   - 节点临时故障（超时/宕机/抖动）时返回包装了 ErrNodeUnavailable 的错误；
//   - 重组期间不同节点可能对同一高度返回不同哈希，由 Fetcher 多数派仲裁。
type Client interface {
	Name() string
	// BlockAt 返回指定高度的区块。
	BlockAt(ctx context.Context, height uint64) (*Block, error)
	// ChainTip 返回节点视角下的最新高度。
	ChainTip(ctx context.Context) (uint64, error)
	// Healthy 返回节点当前是否健康（故障切换排序使用）。
	Healthy() bool
}
