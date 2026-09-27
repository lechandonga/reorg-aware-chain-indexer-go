// Package rpc 提供对节点 RPC 的抽象、故障切换与并发拉取。
package rpc

import (
	"context"
	"errors"

	"github.com/lechandonga/reorg-aware-chain-indexer-go/internal/chain"
)

// ErrTimeout 表示节点请求超时。
var ErrTimeout = errors.New("rpc: 节点超时")

// ErrAllNodesFailed 表示所有节点都不可用。
var ErrAllNodesFailed = errors.New("rpc: 所有节点均失败")

// Node 是单个 RPC 节点的抽象。
type Node interface {
	Name() string
	// BlockByHeight 拉取指定高度的区块；高度不存在时返回 ok=false。
	BlockByHeight(ctx context.Context, height uint64) (chain.Block, bool, error)
	// LatestHeight 返回节点当前最新高度。
	LatestHeight(ctx context.Context) (uint64, error)
}
