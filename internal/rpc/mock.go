package rpc

import (
	"context"
	"sync"
	"time"

	"github.com/lechandonga/reorg-aware-chain-indexer-go/internal/chain"
)

// MockNode 是本地模拟节点，可注入超时与分叉数据。
type MockNode struct {
	mu    sync.Mutex
	name  string
	chain []chain.Block // 当前规范链视图，可整体替换模拟重组
	delay time.Duration
	hang  bool // 模拟永不返回（触发超时）
	Calls int
}

func NewMockNode(name string, blocks []chain.Block) *MockNode {
	return &MockNode{name: name, chain: blocks}
}

func (m *MockNode) Name() string { return m.name }

// SetChain 整体替换节点视图，用于模拟重组。
func (m *MockNode) SetChain(blocks []chain.Block) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.chain = blocks
}

// SetDelay 设置响应延迟。
func (m *MockNode) SetDelay(d time.Duration) { m.mu.Lock(); m.delay = d; m.mu.Unlock() }

// SetHang 让节点永不返回。
func (m *MockNode) SetHang(h bool) { m.mu.Lock(); m.hang = h; m.mu.Unlock() }

func (m *MockNode) BlockByHeight(ctx context.Context, height uint64) (chain.Block, bool, error) {
	m.mu.Lock()
	m.Calls++
	delay, hang := m.delay, m.hang
	m.mu.Unlock()
	if hang {
		<-ctx.Done()
		return chain.Block{}, false, ctx.Err()
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return chain.Block{}, false, ctx.Err()
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if height == 0 || int(height) > len(m.chain) {
		return chain.Block{}, false, nil
	}
	return m.chain[height-1], true, nil
}

func (m *MockNode) LatestHeight(ctx context.Context) (uint64, error) {
	m.mu.Lock()
	m.Calls++
	delay, hang := m.delay, m.hang
	m.mu.Unlock()
	if hang {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return uint64(len(m.chain)), nil
}
