package indexer_test

import (
	"context"
	"testing"
	"time"

	indexer "github.com/lechandonga/reorg-aware-chain-indexer-go"
	mock "github.com/lechandonga/reorg-aware-chain-indexer-go/mock"
)

// TestLongRunBoundedMemory：链持续增长（边生产边索引），并周期性切换节点健康。
// 长时间运行后：所有区块收敛、缓冲与派生计数稳定、PendingCount 始终有界。
func TestLongRunBoundedMemory(t *testing.T) {
	// 先建 5 个初始区块。
	b := mock.NewBuilder()
	var prev = b.Add(0, hashZero(), []indexer.Event{{Key: "genesis", Value: "g"}})
	for h := 1; h <= 5; h++ {
		prev = b.Add(uint64(h), prev, []indexer.Event{{Key: "tx", Value: heightStr(uint64(h))}})
	}
	chain, err := b.Build(prev)
	if err != nil {
		t.Fatal(err)
	}
	n1 := mock.NewNode("n1", chain)
	n2 := mock.NewNode("n2", chain)
	n3 := mock.NewNode("n3", chain)

	collector := indexer.NewLogCollector()
	store := indexer.NewStore()
	maxInFlight := 4
	ix := indexer.New(indexer.Config{Confirmations: 2, MaxInFlight: maxInFlight,
		RunInterval: 5 * time.Millisecond}, store,
		[]indexer.Client{n1, n2, n3}, collector.Logger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = ix.Run(ctx)

	// 生产者：持续追加到 300 个区块。
	const total = 300
	produced := uint64(5)
	doneProd := make(chan struct{})
	go func() {
		defer close(doneProd)
		for produced < total {
			produced++
			chain.Append([]indexer.Event{{Key: "tx", Value: heightStr(produced)}})
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// 干扰者：周期性抖动主节点（故障切换路径被反复执行）。
	go func() {
		for i := 0; i < 40; i++ {
			time.Sleep(7 * time.Millisecond)
			n1.SetHealthy(false)
			time.Sleep(3 * time.Millisecond)
			n1.SetHealthy(true)
		}
	}()

	// 监视者：并确认缓冲与在途量始终不超过有界窗口。
	// 缓冲上限 = maxBuffered（硬上限）；运行中实际应远小于它（窗口≈深度+批量）。
	maxObserved := 0
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		for {
			select {
			case <-doneProd:
				return
			default:
				if p := ix.PendingCount(); p > maxObserved {
					maxObserved = p
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()

	<-doneProd
	<-watchDone

	// 等待索引器追到链尾（深度 2，所以最终确认到 total-2）。
	waitUntil(t, 3*time.Second, func() bool {
		return ix.NextHeight() >= total-1
	})
	ix.Stop()

	finalTip, _ := store.Tip()
	t.Logf("long run: produced=%d, confirmed tip=%d, next=%d, max buffered=%d",
		total, finalTip, ix.NextHeight(), maxObserved)

	// 1) 收敛正确性：h=0..total-3 全部确认（total,total-1,total-2 中前 total-2 满足深度）。
	wantConfirmed := uint64(total) - 2 // 满足 h+2<=total
	for h := uint64(0); h < wantConfirmed; h++ {
		honest, ok := chain.HashAt(h)
		if !ok {
			t.Fatalf("chain missing h=%d", h)
		}
		got, ok := store.HashAt(h)
		if !ok || got != honest {
			t.Fatalf("h=%d not converged to canonical", h)
		}
	}
	// 2) 派生计数：h=0 为 genesis（1 次）；h=1..wantConfirmed 为 tx 区块，
	//    确认到 h=298 即 298 个 tx。长时间运行中反复故障切换不得重复计数。
	txEvents := int(wantConfirmed) // h=1..298 共 298 个
	if c := store.Count("tx"); c != txEvents {
		t.Fatalf("tx count = %d, want %d (no double count over long run)", c, txEvents)
	}
	if c := store.Count("genesis"); c != 1 {
		t.Fatalf("genesis count = %d, want 1", c)
	}
	// 3) 内存有界：运行中缓冲峰值不得超过批量窗口的合理上界。
	//    理论上 ≤ 确认深度+批量+少量在途；给一个宽松但明确的上界 64。
	if maxObserved > 64 {
		t.Fatalf("buffered blocks grew unbounded: peak=%d", maxObserved)
	}
	printLogsHead(t, collector, "长时间运行（仅打印前 25 条）", 25)
}

func hashZero() indexer.Hash { return indexer.Hash{} }

// printLogsHead 只打印前 n 条流水，避免长测试日志爆炸。
func printLogsHead(t *testing.T, c *indexer.LogCollector, title string, n int) {
	t.Helper()
	t.Logf("===== %s =====", title)
	all := c.All()
	if len(all) < n {
		n = len(all)
	}
	for _, e := range all[:n] {
		t.Logf("[%-8s] h=%d node=%s reason=%s %s", e.Kind, e.Height, e.Node, e.Reason, e.Detail)
	}
	t.Logf("... total %d log entries", len(all))
}
