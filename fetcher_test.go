package indexer_test

import (
	"context"
	"errors"
	"testing"
	"time"

	indexer "github.com/lechandonga/reorg-aware-chain-indexer-go"
	mock "github.com/lechandonga/reorg-aware-chain-indexer-go/mock"
)

// TestFailoverOnUnavailable：主节点在 h=2 超时/宕机，备用节点接管，结论一致。
func TestFailoverOnUnavailable(t *testing.T) {
	chain, blks := linearBlocks(t, 4, "fo")
	n1 := mock.NewNode("primary", chain)
	n2 := mock.NewNode("backup", chain)
	n3 := mock.NewNode("backup2", chain)
	// 主节点对 h=2 不可用；三个节点中其余两个仍返回相同哈希。
	n1.FailAt(2, mock.FailUnavailable)

	collector := indexer.NewLogCollector()
	store := indexer.NewStore()
	ix := indexer.New(indexer.Config{Confirmations: 0, MaxInFlight: 2}, store,
		[]indexer.Client{n1, n2, n3}, collector.Logger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ix.Run(ctx); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool {
		_, ok := store.HashAt(4)
		return ok
	})
	ix.Stop()
	printLogs(t, collector, "主节点故障切换流水")

	for h := uint64(0); h <= 4; h++ {
		got, ok := store.HashAt(h)
		if !ok || got != blks[h].Hash {
			t.Fatalf("h=%d must converge to honest chain via failover", h)
		}
	}
}

// TestAllNodesDownThenRecover：全部节点短暂不可用期间不丢状态、不错误落库，
// 恢复后继续推进并收敛。
func TestAllNodesDownThenRecover(t *testing.T) {
	chain, blks := linearBlocks(t, 3, "rec")
	n1 := mock.NewNode("n1", chain)
	n2 := mock.NewNode("n2", chain)

	collector := indexer.NewLogCollector()
	store := indexer.NewStore()
	ix := indexer.New(indexer.Config{Confirmations: 0, MaxInFlight: 2,
		RunInterval: 10 * time.Millisecond}, store,
		[]indexer.Client{n1, n2}, collector.Logger())

	n1.SetHealthy(false)
	n2.SetHealthy(false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = ix.Run(ctx)
	time.Sleep(60 * time.Millisecond) // 经历若干轮“无可用节点”
	if _, ok := store.HashAt(0); ok {
		t.Fatal("nothing must commit while all nodes are down")
	}
	// 恢复后自动追上。
	n1.SetHealthy(true)
	n2.SetHealthy(true)
	waitUntil(t, time.Second, func() bool {
		_, ok := store.HashAt(3)
		return ok
	})
	ix.Stop()
	printLogs(t, collector, "全节点宕机+恢复流水")

	var sawNoHealthy bool
	for _, r := range collector.Reasons() {
		if r == indexer.ReasonNoHealthyNode {
			sawNoHealthy = true
		}
	}
	if !sawNoHealthy {
		t.Fatal("must record no_healthy_node while down")
	}
	for h := uint64(0); h <= 3; h++ {
		if got, ok := store.HashAt(h); !ok || got != blks[h].Hash {
			t.Fatalf("h=%d must converge after recovery", h)
		}
	}
}

// TestContradictingNodeQuarantined：单个节点返回矛盾数据，多数派胜出且结果一致。
func TestContradictingNodeQuarantined(t *testing.T) {
	chain, blks := linearBlocks(t, 4, "q")
	good1 := mock.NewNode("good-1", chain)
	good2 := mock.NewNode("good-2", chain)
	liar := mock.NewNode("liar", chain)
	// 恶意节点在所有高度都伪造区块；2 个诚实节点占多数。
	for h := uint64(0); h <= 4; h++ {
		liar.FailAt(h, mock.FailConflicting)
	}

	collector := indexer.NewLogCollector()
	store := indexer.NewStore()
	ix := indexer.New(indexer.Config{Confirmations: 0, MaxInFlight: 3,
		RunInterval: 10 * time.Millisecond}, store,
		[]indexer.Client{good1, good2, liar}, collector.Logger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = ix.Run(ctx)
	waitUntil(t, 2*time.Second, func() bool {
		_, ok := store.HashAt(4)
		return ok
	})
	ix.Stop()
	printLogs(t, collector, "矛盾节点隔离流水")

	for h := uint64(0); h <= 4; h++ {
		if got, ok := store.HashAt(h); !ok || got != blks[h].Hash {
			t.Fatalf("h=%d must follow honest majority, got %v", h, got)
		}
	}
	var quarantines int
	for _, r := range collector.Reasons() {
		if r == indexer.ReasonNodeQuarantine {
			quarantines++
		}
	}
	if quarantines == 0 {
		t.Fatal("liar node must be quarantined with distinguishable reason")
	}
}

// TestFetcherDirectMajority：直接验证仲裁函数在“1 坏 2 好”时返回多数派哈希。
func TestFetcherDirectMajority(t *testing.T) {
	chain, blks := linearBlocks(t, 2, "dir")
	good := mock.NewNode("good", chain)
	liar := mock.NewNode("liar", chain)
	liar.FailAt(1, mock.FailConflicting)
	good2 := mock.NewNode("good2", chain)

	coll := indexer.NewLogCollector()
	blk, err := indexer.FetchBlockWithFailover(context.Background(),
		[]indexer.Client{good, liar, good2}, 1, map[string]bool{}, coll.Logger())
	if err != nil {
		t.Fatal(err)
	}
	if blk.Hash != blks[1].Hash {
		t.Fatal("majority result must equal honest hash")
	}
	var q int
	for _, r := range coll.Reasons() {
		if r == indexer.ReasonNodeQuarantine {
			q++
		}
	}
	if q != 1 {
		t.Fatalf("liar must be quarantined once, got %d", q)
	}

	// 全部不可用 -> ErrNoHealthyNode。
	down1 := mock.NewNode("d1", chain)
	down2 := mock.NewNode("d2", chain)
	down1.SetHealthy(false)
	down2.SetHealthy(false)
	_, err = indexer.FetchBlockWithFailover(context.Background(),
		[]indexer.Client{down1, down2}, 0, map[string]bool{}, coll.Logger())
	if !errors.Is(err, indexer.ErrNoHealthyNode) {
		t.Fatalf("want ErrNoHealthyNode, got %v", err)
	}
}

// waitUntil 轮询条件直到成立或超时。
func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
