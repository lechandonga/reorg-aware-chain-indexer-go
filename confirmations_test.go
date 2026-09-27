package indexer_test

import (
	"testing"

	indexer "github.com/lechandonga/reorg-aware-chain-indexer-go"
)

func TestConfirmationDepth(t *testing.T) {
	_, blks := linearBlocks(t, 6, "conf")
	collector := indexer.NewLogCollector()
	store := indexer.NewStore()
	ix := indexer.New(indexer.Config{Confirmations: 2}, store, nil, collector.Logger())

	// 链头只有 2 时：h=0 上方已有 2 块（tip-h>=2）可以落库，h=1 不行。
	_ = ix.Submit(blks[0])
	_ = ix.Submit(blks[1])
	ix.OfferTip(2)
	if _, ok := store.HashAt(0); !ok {
		t.Fatal("h=0 should commit: tip-h = 2 >= confirmations(2)")
	}
	if _, ok := store.HashAt(1); ok {
		t.Fatal("h=1 must NOT commit (tip-h=1 < 2)")
	}
	if ix.PendingCount() != 1 {
		t.Fatalf("unconfirmed h=1 must stay buffered, pending=%d", ix.PendingCount())
	}
	// 链头推进到 4 并补齐 h=2：h=1,2 相继确认；未投递的 h=3,4 不受影响。
	ix.OfferTip(4)
	_ = ix.Submit(blks[2])
	for h := uint64(0); h <= 2; h++ {
		if _, ok := store.HashAt(h); !ok {
			t.Fatalf("h=%d should be committed at tip=4 (depth 2)", h)
		}
	}
	if _, ok := store.HashAt(3); ok {
		t.Fatal("h=3 never delivered and must not exist")
	}
	// 只有达到确认深度的区块才正式落库：事件计数与已确认高度一致。
	if store.Count("conf:tx") != 3 {
		t.Fatalf("confirmed tx count = %d, want 3", store.Count("conf:tx"))
	}
	printLogs(t, collector, "确认深度流水")

	// 继续推进链头并补齐区块：tip=6 时仅 h<=4 达到 2 确认，h=5,6 仍缓冲。
	for h := uint64(3); h <= 6; h++ {
		_ = ix.Submit(blks[h])
	}
	ix.OfferTip(6)
	if store.Count("conf:tx") != 5 {
		t.Fatalf("confirmed tx count = %d, want 5 (h=0..4)", store.Count("conf:tx"))
	}
	if ix.NextHeight() != 5 {
		t.Fatalf("next = %d, want 5", ix.NextHeight())
	}
	if ix.PendingCount() != 2 {
		t.Fatalf("h=5,6 must remain buffered, pending=%d", ix.PendingCount())
	}
}
