package rpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lechandonga/reorg-aware-chain-indexer-go/internal/chain"
)

// Client 在多个节点间做故障切换与一致性校验。
type Client struct {
	nodes   []Node
	timeout time.Duration
}

// NewClient 构建故障切换客户端，nodes 按优先级排序。
func NewClient(timeoutMs int64, nodes ...Node) *Client {
	return &Client{nodes: nodes, timeout: time.Duration(timeoutMs) * time.Millisecond}
}

type queryResult struct {
	node  string
	block chain.Block
	ok    bool
	err   error
}

// queryAll 并发问询所有节点（带超时），单个节点超时/挂起不影响其它节点。
func (c *Client) queryAll(ctx context.Context, height uint64) []queryResult {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	res := make([]queryResult, len(c.nodes))
	var wg sync.WaitGroup
	for i, n := range c.nodes {
		wg.Add(1)
		go func(i int, n Node) {
			defer wg.Done()
			b, ok, err := n.BlockByHeight(ctx, height)
			res[i] = queryResult{node: n.Name(), block: b, ok: ok, err: err}
		}(i, n)
	}
	wg.Wait()
	return res
}

// BlockByHeight 并发问询全部节点，按多数决给出一致结果；
// 节点超时自动被跳过（故障切换），数据互相矛盾且无多数时返回错误。
func (c *Client) BlockByHeight(ctx context.Context, height uint64) (chain.Block, bool, error) {
	res := c.queryAll(ctx, height)
	votes := map[string]int{}
	var byHash map[string]chain.Block = map[string]chain.Block{}
	total := 0
	for _, r := range res {
		if r.err != nil || !r.ok {
			continue
		}
		votes[r.block.Hash]++
		byHash[r.block.Hash] = r.block
		total++
	}
	if total == 0 {
		return chain.Block{}, false, ErrAllNodesFailed
	}
	// 严格多数决：得票超过实际响应数的一半才采纳。
	// 挂起/超时的节点不计入响应数，自然完成故障切换；
	// 矛盾且无任何一方过半（如 1v1）时报错而非随机选择。
	for hash, v := range votes {
		if v > total/2 {
			return byHash[hash], true, nil
		}
	}
	return chain.Block{}, false, fmt.Errorf("rpc: 高度 %d 节点数据互相矛盾，无多数派 (votes=%v)", height, votes)
}

// LatestHeight 取健康节点返回的最大高度；全部失败时返回 ErrAllNodesFailed。
func (c *Client) LatestHeight(ctx context.Context) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	type r struct {
		h   uint64
		err error
	}
	ch := make(chan r, len(c.nodes))
	for _, n := range c.nodes {
		go func(n Node) {
			h, err := n.LatestHeight(ctx)
			ch <- r{h, err}
		}(n)
	}
	var max uint64
	ok := false
	for range c.nodes {
		rr := <-ch
		if rr.err == nil {
			ok = true
			if rr.h > max {
				max = rr.h
			}
		}
	}
	if !ok {
		return 0, ErrAllNodesFailed
	}
	return max, nil
}

// FetchRange 并发拉取 [from, to] 高度的区块；结果写入定长切片对应下标，
// 返回时严格按高度升序，并发不会导致乱序。任一高度失败则整体失败。
func (c *Client) FetchRange(ctx context.Context, from, to uint64) ([]chain.Block, error) {
	if to < from {
		return nil, nil
	}
	out := make([]chain.Block, to-from+1)
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4) // 限制并发度，内存占用有界
	errCh := make(chan error, 1)
	for h := from; h <= to; h++ {
		wg.Add(1)
		go func(h uint64) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			b, ok, err := c.BlockByHeight(ctx, h)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			if !ok {
				select {
				case errCh <- fmt.Errorf("rpc: 高度 %d 不存在", h):
				default:
				}
				return
			}
			out[h-from] = b // 下标唯一，无数据竞争
		}(h)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
	}
	return out, nil
}
