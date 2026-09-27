package indexer_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/lechandonga/reorg-aware-chain-indexer-go/internal/chain"
	"github.com/lechandonga/reorg-aware-chain-indexer-go/internal/indexer"
	"github.com/lechandonga/reorg-aware-chain-indexer-go/internal/rpc"
)

func mkChain(prefix string, from, to uint64, parent string) []chain.Block {
	var out []chain.Block
	for h := from; h <= to; h++ {
		hash := fmt.Sprintf("%s-%d", prefix, h)
		out = append(out, chain.Block{Height: h, Hash: hash, ParentHash: parent,
			Payload: []string{"tx-" + hash}})
		parent = hash
	}
	return out
}

// TestEndToEndReorgConvergence 模拟完整流水线：
// 并发拉取 → 乱序归一 → 确认落库 → 节点视图重组 → 收敛到新链。
func TestEndToEndReorgConvergence(t *testing.T) {
	logger := log.New(os.Stdout, "[e2e] ", log.Lmicroseconds)
	oldChain := mkChain("A", 1, 20, "genesis")
	// 重组：高度 16 起换成 B 分支，链更长
	newChain := append(mkChain("A", 1, 15, "genesis"), mkChain("B", 16, 24, "A-15")...)

	n1 := rpc.NewMockNode("n1", oldChain)
	n2 := rpc.NewMockNode("n2", oldChain)
	n1.SetDelay(2 * time.Millisecond) // 模拟慢节点
	client := rpc.NewClient(200, n1, n2)

	store := chain.NewMemoryStore()
	ix := indexer.New(store, 1, 3, logger)
	ctx := context.Background()

	// 阶段 1：拉取旧链 1..20（FetchRange 并发，内部按高度排序）
	blocks, err := client.FetchRange(ctx, 1, 20)
	if err != nil {
		t.Fatalf("阶段1拉取失败: %v", err)
	}
	for _, b := range blocks {
		ix.Ingest(b)
	}
	ix.Finalize(20)
	if store.Len() != 17 { // 20-3=17 已确认
		t.Fatalf("阶段1落库=%d want 17", store.Len())
	}

	// 阶段 2：节点视图重组（两个节点都切换到新链）
	n1.SetChain(newChain)
	n2.SetChain(newChain)
	latest, err := client.LatestHeight(ctx)
	if err != nil {
		t.Fatalf("LatestHeight: %v", err)
	}
	if latest != 24 {
		t.Fatalf("latest=%d want 24", latest)
	}
	// 从已落库高度附近重新拉取，触发重组判定
	blocks, err = client.FetchRange(ctx, 16, 24)
	if err != nil {
		t.Fatalf("阶段2拉取失败: %v", err)
	}
	for _, b := range blocks {
		ix.Ingest(b)
	}
	ix.Finalize(latest)

	// 收敛验证：最终链 == newChain 的已确认前缀（24-3=21）
	if store.Len() != 21 {
		t.Fatalf("阶段2落库=%d want 21", store.Len())
	}
	for h := 1; h <= 21; h++ {
		got, _ := store.Get(uint64(h))
		want := newChain[h-1]
		if got.Hash != want.Hash {
			t.Fatalf("高度 %d 未收敛: got %s want %s", h, got.Hash, want.Hash)
		}
	}
	// 无重复计入
	total := 0
	for h := 1; h <= store.Len(); h++ {
		b, _ := store.Get(uint64(h))
		total += len(b.Payload)
	}
	if total != 21 {
		t.Fatalf("payload 重复计入: %d", total)
	}
}

// TestLongRunStable 长时间运行：持续推进+多次抖动，状态有界且最终一致。
func TestLongRunStable(t *testing.T) {
	logger := log.New(os.Stdout, "[soak] ", log.Lmicroseconds)
	const rounds = 30
	const perRound = 20
	full := mkChain("A", 1, rounds*perRound, "genesis")
	n1 := rpc.NewMockNode("n1", full)
	n2 := rpc.NewMockNode("n2", full)
	client := rpc.NewClient(100, n1, n2)
	store := chain.NewMemoryStore()
	ix := indexer.New(store, 1, 2, logger)
	ctx := context.Background()

	for r := 0; r < rounds; r++ {
		from := uint64(r*perRound + 1)
		to := uint64((r + 1) * perRound)
		blocks, err := client.FetchRange(ctx, from, to)
		if err != nil {
			t.Fatalf("轮次 %d 拉取失败: %v", r, err)
		}
		// 抖动：重复投递一遍 + 倒序再投递
		for _, b := range blocks {
			ix.Ingest(b)
		}
		for i := len(blocks) - 1; i >= 0; i-- {
			ix.Ingest(blocks[i])
		}
		ix.Finalize(to)
	}
	want := rounds*perRound - 2 // 确认深度 2
	if store.Len() != want {
		t.Fatalf("落库=%d want %d", store.Len(), want)
	}
	for h := 1; h <= store.Len(); h++ {
		b, _ := store.Get(uint64(h))
		if b.Hash != fmt.Sprintf("A-%d", h) {
			t.Fatalf("高度 %d 不一致: %s", h, b.Hash)
		}
	}
}
