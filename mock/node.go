package mock

import (
	"context"
	"sync"
	"time"

	indexer "github.com/lechandonga/reorg-aware-chain-indexer-go"
)

// FailMode 描述节点在“故障高度”上的行为。
type FailMode int

const (
	// FailUnavailable：返回不可用错误（模拟超时/宕机/抖动）。
	FailUnavailable FailMode = iota
	// FailConflicting：返回一个同高度但哈希不同的伪造区块
	// （模拟节点间互相矛盾的数据，用于多数派隔离测试）。
	FailConflicting
)

// Node 是可编排故障的本地模拟节点，实现 indexer.Client。
// 所有控制字段由 mu 保护，允许测试线程在索引器并发拉取时安全地
// 切换健康状态/注入故障（-race 安全）。
type Node struct {
	name  string
	chain *Chain

	mu          sync.RWMutex
	healthy     bool
	latency     time.Duration
	failHeights map[uint64]FailMode // 指定高度强制故障
	failEveryN  uint64              // 每 N 次请求故障一次（0 关闭）
	failMode    FailMode
	reqCount    uint64
}

// NewNode 创建服务于指定链视图的模拟节点。
func NewNode(name string, chain *Chain) *Node {
	return &Node{
		name:        name,
		chain:       chain,
		healthy:     true,
		failHeights: make(map[uint64]FailMode),
		failMode:    FailUnavailable,
	}
}

// Name 返回节点名。
func (n *Node) Name() string { return n.name }

// Healthy 报告节点健康状态（索引器据此排故障切换顺序）。
func (n *Node) Healthy() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.healthy
}

// SetHealthy 切换节点健康状态（模拟宕机/恢复）。
func (n *Node) SetHealthy(ok bool) {
	n.mu.Lock()
	n.healthy = ok
	n.mu.Unlock()
}

// SetLatency 注入每次调用的固定延迟（0 取消）。配合短 ctx 超时可模拟慢节点。
func (n *Node) SetLatency(d time.Duration) {
	n.mu.Lock()
	n.latency = d
	n.mu.Unlock()
}

// FailAt 让节点在指定高度按 mode 行为故障；FailMode 不传则默认 FailUnavailable。
func (n *Node) FailAt(height uint64, mode ...FailMode) {
	m := FailUnavailable
	if len(mode) > 0 {
		m = mode[0]
	}
	n.mu.Lock()
	n.failHeights[height] = m
	n.mu.Unlock()
}

// ClearFailAt 取消某高度的强制故障。
func (n *Node) ClearFailAt(height uint64) {
	n.mu.Lock()
	delete(n.failHeights, height)
	n.mu.Unlock()
}

// FailEveryN 周期性故障（模拟短暂抖动）；0 表示关闭。
func (n *Node) FailEveryN(k uint64, mode ...FailMode) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.failEveryN = k
	if len(mode) > 0 {
		n.failMode = mode[0]
	}
}

// snapshot 在锁内一次性取出本次调用需要的全部控制参数。
type nodeState struct {
	healthy   bool
	latency   time.Duration
	mode      FailMode
	forced    FailMode
	hasForced bool
	reqNo     uint64
}

func (n *Node) beginCall(height uint64) nodeState {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reqCount++
	st := nodeState{
		healthy: n.healthy,
		latency: n.latency,
		mode:    n.failMode,
		reqNo:   n.reqCount,
	}
	if m, bad := n.failHeights[height]; bad {
		st.forced, st.hasForced = m, true
	}
	return st
}

// BlockAt 实现 indexer.Client。
func (n *Node) BlockAt(ctx context.Context, height uint64) (*indexer.Block, error) {
	st := n.beginCall(height)
	// 注入延迟（尊重 ctx 取消）。
	if st.latency > 0 {
		select {
		case <-time.After(st.latency):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if !st.healthy {
		return nil, ErrNodeUnavailable
	}
	if st.hasForced {
		return n.faultyBlock(height, st.forced)
	}
	if n.periodicFault(st) {
		return n.faultyBlock(height, st.mode)
	}
	blk, ok := n.chain.BlockAtHeight(height)
	if !ok {
		return nil, indexer.ErrNotFound
	}
	blk.NodeHint = n.name
	return blk, nil
}

// periodicFault 判断本次（锁内已自增的）请求序号是否命中周期性故障。
func (n *Node) periodicFault(st nodeState) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.failEveryN > 0 && st.reqNo%n.failEveryN == 0
}

// faultyBlock 按故障模式产生不可用错误或矛盾区块。
func (n *Node) faultyBlock(height uint64, mode FailMode) (*indexer.Block, error) {
	if mode == FailUnavailable {
		return nil, ErrNodeUnavailable
	}
	// 伪造一个内容被篡改的区块：保留高度，塞入不同事件，
	// 使其哈希与诚实节点必然不同。
	parent := indexer.Hash{}
	if height > 0 {
		if p, ok := n.chain.HashAt(height - 1); ok {
			parent = p
		}
	}
	fake := &indexer.Block{
		Height:   height,
		Parent:   parent,
		Events:   []indexer.Event{{Key: "__byzantine__", Value: n.name}},
		NodeHint: n.name,
	}
	fake.Hash = ComputeHash(height, parent, fake.Events)
	return fake, nil
}

// ChainTip 实现 indexer.Client。
func (n *Node) ChainTip(ctx context.Context) (uint64, error) {
	n.mu.RLock()
	latency := n.latency
	healthy := n.healthy
	n.mu.RUnlock()
	if latency > 0 {
		select {
		case <-time.After(latency):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if !healthy {
		return 0, ErrNodeUnavailable
	}
	return n.chain.Tip(), nil
}

var _ indexer.Client = (*Node)(nil)
