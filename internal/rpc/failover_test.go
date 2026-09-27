package rpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lechandonga/reorg-aware-chain-indexer-go/internal/chain"
)

func mkChain(prefix string, from, to uint64, parent string) []chain.Block {
	var out []chain.Block
	for h := from; h <= to; h++ {
		hash := fmt.Sprintf("%s-%d", prefix, h)
		out = append(out, chain.Block{Height: h, Hash: hash, ParentHash: parent})
		parent = hash
	}
	return out
}

func TestFailoverOnHang(t *testing.T) {
	blocks := mkChain("A", 1, 10, "genesis")
	bad := NewMockNode("bad", blocks)
	bad.SetHang(true) // 永不返回
	good := NewMockNode("good", blocks)
	c := NewClient(50, bad, good)

	start := time.Now()
	b, ok, err := c.BlockByHeight(context.Background(), 5)
	if err != nil || !ok {
		t.Fatalf("故障切换失败: ok=%v err=%v", ok, err)
	}
	if b.Hash != "A-5" {
		t.Fatalf("hash 不符: %s", b.Hash)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("超时未被约束: %v", elapsed)
	}
	h, err := c.LatestHeight(context.Background())
	if err != nil || h != 10 {
		t.Fatalf("LatestHeight=%d err=%v", h, err)
	}
}

func TestConflictingNodesMajority(t *testing.T) {
	honest := mkChain("A", 1, 10, "genesis")
	forked := mkChain("X", 1, 10, "genesis") // 矛盾数据
	n1 := NewMockNode("n1", honest)
	n2 := NewMockNode("n2", honest)
	n3 := NewMockNode("n3", forked)
	c := NewClient(50, n1, n2, n3)

	b, ok, err := c.BlockByHeight(context.Background(), 7)
	if err != nil || !ok {
		t.Fatalf("多数决失败: %v", err)
	}
	if b.Hash != "A-7" {
		t.Fatalf("应取多数派 A-7, got %s", b.Hash)
	}

	// 1 诚实 vs 1 矛盾：无多数派 → 必须报错而不是随机选一个
	c2 := NewClient(50, NewMockNode("a", honest), NewMockNode("b", forked))
	_, _, err = c2.BlockByHeight(context.Background(), 7)
	if err == nil {
		t.Fatal("矛盾无多数时必须返回错误")
	}
	t.Logf("矛盾判定: %v", err)
}

func TestAllNodesDown(t *testing.T) {
	n1 := NewMockNode("n1", nil)
	n1.SetHang(true)
	n2 := NewMockNode("n2", nil)
	n2.SetHang(true)
	c := NewClient(30, n1, n2)
	_, _, err := c.BlockByHeight(context.Background(), 1)
	if err != ErrAllNodesFailed {
		t.Fatalf("期望 ErrAllNodesFailed, got %v", err)
	}
}

func TestFetchRangeOrderedAndConcurrent(t *testing.T) {
	blocks := mkChain("A", 1, 50, "genesis")
	// 节点带不同延迟，并发拉取也不能乱序
	n1 := NewMockNode("n1", blocks)
	n1.SetDelay(5 * time.Millisecond)
	n2 := NewMockNode("n2", blocks)
	c := NewClient(100, n1, n2)

	out, err := c.FetchRange(context.Background(), 1, 50)
	if err != nil {
		t.Fatalf("FetchRange: %v", err)
	}
	if len(out) != 50 {
		t.Fatalf("数量不符: %d", len(out))
	}
	for i, b := range out {
		wantH := uint64(i + 1)
		if b.Height != wantH || b.Hash != fmt.Sprintf("A-%d", wantH) {
			t.Fatalf("下标 %d 乱序: height=%d hash=%s", i, b.Height, b.Hash)
		}
	}
}

// TestFetchRangeReorgView 节点视图中途重组：客户端逐高度问询，
// 多数派（已切换的 n2/n3）给出新链结果。
func TestFetchRangeReorgView(t *testing.T) {
	old := mkChain("A", 1, 10, "genesis")
	newChain := append(mkChain("A", 1, 5, "genesis"), mkChain("B", 6, 10, "A-5")...)
	stale := NewMockNode("stale", old)
	n2 := NewMockNode("n2", newChain)
	n3 := NewMockNode("n3", newChain)
	c := NewClient(50, stale, n2, n3)
	b, ok, err := c.BlockByHeight(context.Background(), 8)
	if err != nil || !ok {
		t.Fatalf("重组视图多数决失败: %v", err)
	}
	if b.Hash != "B-8" {
		t.Fatalf("应收敛到新链 B-8, got %s", b.Hash)
	}
}
