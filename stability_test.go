package indexer_test

import (
	"context"
	"testing"
	"time"

	indexer "github.com/lechandonga/reorg-aware-chain-indexer-go"
	mock "github.com/lechandonga/reorg-aware-chain-indexer-go/mock"
)

// TestBoundedBufferRejectsFarAhead：乱序投递远超 next 的高度时，
// 缓冲不能无限增长，超出窗口以可区分的 gap_ahead 原因拒绝。
func TestBoundedBufferRejectsFarAhead(t *testing.T) {
	_, blks := linearBlocks(t, 0, "bnd")
	_ = blks
	b := mock.NewBuilder()
	var prev indexer.Hash
	count := 6000
	for h := 0; h <= count; h++ {
		evs := []indexer.Event{{Key: "x", Value: heightStr(uint64(h))}}
		prev = b.Add(uint64(h), prev, evs)
	}
	chain, err := b.Build(prev)
	if err != nil {
		t.Fatal(err)
	}
	collector := indexer.NewLogCollector()
	ix := indexer.New(indexer.Config{Confirmations: 0}, indexer.NewStore(), nil, collector.Logger())

	// 从后往前乱序投递，制造大量“前方缺口”区块。
	node := mock.NewNode("n", chain)
	var rejected int
	for h := uint64(count); h > 10; h-- {
		blk, err := node.BlockAt(context.Background(), h)
		if err != nil {
			t.Fatal(err)
		}
		if err := ix.Submit(blk); err != nil {
			rejected++
		}
		if h == 0 { // 防御 uint64 下溢（不会到这）
			break
		}
	}
	printLogs(t, collector, "有界缓冲（仅打印前段，后略）")
	if ix.PendingCount() > 4096 {
		t.Fatalf("buffer unbounded: %d", ix.PendingCount())
	}
	if rejected == 0 {
		t.Fatal("far-ahead blocks beyond window must be rejected with gap_ahead")
	}
	var sawGap bool
	for _, r := range collector.Reasons() {
		if r == indexer.ReasonGapAhead {
			sawGap = true
		}
	}
	if !sawGap {
		t.Fatal("must record gap_ahead reasons")
	}
}

// TestRuntimeReorgConvergence：后台循环运行中，节点视图切换到更长的侧链，
// 索引状态必须自动收敛到新规范链（无需重启）。
func TestRuntimeReorgConvergence(t *testing.T) {
	f := buildFork(t)
	chain, _ := f.builder.Build(f.a[2]) // 初始 A 链 tip=2
	n1 := mock.NewNode("n1", chain)
	n2 := mock.NewNode("n2", chain)

	collector := indexer.NewLogCollector()
	store := indexer.NewStore()
	ix := indexer.New(indexer.Config{Confirmations: 0, MaxInFlight: 3,
		RunInterval: 10 * time.Millisecond}, store,
		[]indexer.Client{n1, n2}, collector.Logger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = ix.Run(ctx)

	// 等 A 链 0..2 全部确认。
	waitUntil(t, time.Second, func() bool {
		_, ok := store.HashAt(2)
		return ok
	})

	// 运行中切换到 B 链（tip=3，h=1 起不同）。
	if err := chain.SwitchTo(f.b[3]); err != nil {
		t.Fatal(err)
	}
	// 后台循环会拉到 h=1..3 的新哈希并触发回滚重放，最终收敛。
	waitUntil(t, 2*time.Second, func() bool {
		h2, ok := store.HashAt(2)
		h3, ok3 := store.HashAt(3)
		return ok && ok3 && h2 == f.b[2] && h3 == f.b[3]
	})
	ix.Stop()
	printLogs(t, collector, "运行中重组收敛流水")

	for h := uint64(0); h <= 3; h++ {
		got, ok := store.HashAt(h)
		if !ok || got != f.b[h] {
			t.Fatalf("h=%d must converge to chain B, got ok=%v", h, ok)
		}
	}
	if store.Count("old") != 0 || store.Count("new") != 3 {
		t.Fatalf("counts must converge: old=%d new=%d", store.Count("old"), store.Count("new"))
	}
	found := false
	for _, e := range collector.All() {
		if e.Kind == indexer.LogReorg {
			found = true
		}
	}
	if !found {
		t.Fatal("must log reorg decision during runtime switch")
	}
}
