package indexer

import "errors"

// 预定义错误：调用方可用 errors.Is 区分不同的拒绝/失败原因。
var (
	// ErrGapAhead 表示请求提交的区块高度领先于下一个应处理高度，存在缺口。
	ErrGapAhead = errors.New("indexer: height gap ahead, waiting for missing blocks")
	// ErrAlreadyCanonical 表示该区块已在规范链上（重复投递）。
	ErrAlreadyCanonical = errors.New("indexer: block already indexed on canonical chain")
	// ErrAlreadyBuffered 表示该区块已在乱序缓冲中。
	ErrAlreadyBuffered = errors.New("indexer: block already buffered")
	// ErrHashConflict 表示同一高度出现了与已知区块不同的哈希，触发重组判定。
	ErrHashConflict = errors.New("indexer: conflicting hash at same height")
	// ErrNodeQuarantine 表示节点返回的数据与多数派矛盾，被隔离并切换。
	ErrNodeQuarantine = errors.New("indexer: node quarantined due to conflicting data")
	// ErrNoHealthyNode 表示所有节点当前都不可用。
	ErrNoHealthyNode = errors.New("indexer: no healthy node available")
	// ErrStopped 表示索引器已停止。
	ErrStopped = errors.New("indexer: stopped")
	// ErrNodeUnavailable 表示节点超时/宕机/临时抖动，可触发故障切换。
	ErrNodeUnavailable = errors.New("indexer: node unavailable")
	// ErrNotFound 表示节点上不存在该高度的区块（高于链头）。
	ErrNotFound = errors.New("indexer: block not found")
)
