package mock

import (
	"context"
	"testing"

	indexer "github.com/lechandonga/reorg-aware-chain-indexer-go"
)

// buildLinear 构建一条 genesis + n 个区块的线性链，事件为 ("tx", "h<height>")。
// 返回 (chain, hashes)。
func buildLinear(t *testing.T, n int) (*Chain, []indexer.Hash) {
	t.Helper()
	b := NewBuilder()
	hashes := make([]indexer.Hash, n+1)
	hashes[0] = b.Add(0, indexer.Hash{}, []indexer.Event{{Key: "genesis", Value: "g"}})
	for h := 1; h <= n; h++ {
		hashes[h] = b.Add(uint64(h), hashes[h-1],
			[]indexer.Event{{Key: "tx", Value: "h" + u(uint64(h))}})
	}
	c, err := b.Build(hashes[n])
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return c, hashes
}

func u(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func TestChainLinearAndTip(t *testing.T) {
	c, hashes := buildLinear(t, 5)
	if c.Tip() != 5 {
		t.Fatalf("tip = %d, want 5", c.Tip())
	}
	for h := uint64(0); h <= 5; h++ {
		got, ok := c.HashAt(h)
		if !ok || got != hashes[h] {
			t.Fatalf("hash at %d mismatch", h)
		}
		blk, ok := c.BlockAtHeight(h)
		if !ok || blk.Parent != func() indexer.Hash {
			if h == 0 {
				return indexer.Hash{}
			}
			return hashes[h-1]
		}() {
			t.Fatalf("parent linkage wrong at %d", h)
		}
	}
	if _, ok := c.HashAt(6); ok {
		t.Fatal("height beyond tip must not exist")
	}
}

func TestHashDeterministic(t *testing.T) {
	// 相同内容必须产生相同哈希（重复投递幂等的基础）。
	h1 := ComputeHash(3, indexer.Hash{1}, []indexer.Event{{Key: "a", Value: "b"}})
	h2 := ComputeHash(3, indexer.Hash{1}, []indexer.Event{{Key: "a", Value: "b"}})
	h3 := ComputeHash(3, indexer.Hash{2}, []indexer.Event{{Key: "a", Value: "b"}})
	if h1 != h2 {
		t.Fatal("identical content must hash equal")
	}
	if h1 == h3 {
		t.Fatal("different parent must hash differently")
	}
}

func TestChainSwitchToSideChain(t *testing.T) {
	b := NewBuilder()
	g := b.Add(0, indexer.Hash{}, []indexer.Event{{Key: "genesis", Value: "g"}})
	a1 := b.Add(1, g, []indexer.Event{{Key: "tx", Value: "A1"}})
	a2 := b.Add(2, a1, []indexer.Event{{Key: "tx", Value: "A2"}})
	// 在高度 1 分叉出侧链 b1,b2,b3。
	b1 := b.Add(1, g, []indexer.Event{{Key: "tx", Value: "B1"}})
	b2 := b.Add(2, b1, []indexer.Event{{Key: "tx", Value: "B2"}})
	b3 := b.Add(3, b2, []indexer.Event{{Key: "tx", Value: "B3"}})

	c, err := b.Build(a2)
	if err != nil {
		t.Fatal(err)
	}
	if h, _ := c.HashAt(1); h != a1 {
		t.Fatal("initial view must be chain A")
	}
	if err := c.SwitchTo(b3); err != nil {
		t.Fatalf("switch: %v", err)
	}
	if c.Tip() != 3 {
		t.Fatalf("tip after switch = %d", c.Tip())
	}
	// genesis 与 A 链共享；高度 1 起换成 B 链。
	if h0, _ := c.HashAt(0); h0 != g {
		t.Fatal("shared genesis must survive reorg")
	}
	if h1, _ := c.HashAt(1); h1 != b1 {
		t.Fatal("h1 must switch to side chain")
	}
	if h2, _ := c.HashAt(2); h2 != b2 {
		t.Fatal("h2 must switch to side chain")
	}
	blk, _ := c.BlockAtHeight(1)
	if blk.Parent != g {
		t.Fatal("side block parent must point to shared genesis")
	}

	// Node 服务于该视图，重组后 RPC 立即返回新链。
	n := NewNode("n1", c)
	got, err := n.BlockAt(context.Background(), 1)
	if err != nil || got.Hash != b1 {
		t.Fatalf("node should serve new view, got %v", err)
	}
}

func TestNodeUnavailableAndConflict(t *testing.T) {
	c, _ := buildLinear(t, 3)
	n := NewNode("n1", c)

	n.SetHealthy(false)
	if _, err := n.BlockAt(context.Background(), 1); err != ErrNodeUnavailable {
		t.Fatalf("want ErrNodeUnavailable, got %v", err)
	}
	if n.Healthy() {
		t.Fatal("node must report unhealthy")
	}
	n.SetHealthy(true)

	n.FailAt(2, FailConflicting)
	blk, err := n.BlockAt(context.Background(), 2)
	if err != nil {
		t.Fatalf("conflict mode returns fake block, got err %v", err)
	}
	honest, _ := c.HashAt(2)
	if blk.Hash == honest {
		t.Fatal("byzantine block hash must differ from honest chain")
	}

	n.FailAt(3, FailUnavailable)
	if _, err := n.BlockAt(context.Background(), 3); err != ErrNodeUnavailable {
		t.Fatalf("want unavailable, got %v", err)
	}
}
