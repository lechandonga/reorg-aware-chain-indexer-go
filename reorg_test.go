package indexer_test

import (
	"testing"

	indexer "github.com/lechandonga/reorg-aware-chain-indexer-go"
	mock "github.com/lechandonga/reorg-aware-chain-indexer-go/mock"
)

// forkFixture 构建在高度 1 分叉的两条链：
//
//	g0 - a1 - a2 - a3   （旧链，事件 key=old:n）
//	  \ b1 - b2 - b3 - b4 （新链，事件 key=new:n，分叉后更长）
type forkFixture struct {
	builder *mock.Builder
	g       indexer.Hash
	a       [4]indexer.Hash // a[0]=g, a[1..3] 高度1..3
	b       [5]indexer.Hash // b[0]=g, b[1..4] 高度1..4
}

func buildFork(t *testing.T) forkFixture {
	t.Helper()
	b := mock.NewBuilder()
	var f forkFixture
	f.builder = b
	f.g = b.Add(0, indexer.Hash{}, []indexer.Event{{Key: "genesis", Value: "g"}})
	f.a[0] = f.g
	f.b[0] = f.g
	prev := f.g
	for h := 1; h <= 3; h++ {
		f.a[h] = b.Add(uint64(h), prev,
			[]indexer.Event{{Key: "old", Value: heightStr(uint64(h))}})
		prev = f.a[h]
	}
	prev = f.g
	for h := 1; h <= 4; h++ {
		f.b[h] = b.Add(uint64(h), prev,
			[]indexer.Event{{Key: "new", Value: heightStr(uint64(h))}})
		prev = f.b[h]
	}
	return f
}

func (f forkFixture) blk(hash indexer.Hash) *indexer.Block {
	return f.builder.Block(hash)
}

func TestReorgConfirmedRollbackAndConverge(t *testing.T) {
	f := buildFork(t)
	chain, err := f.builder.Build(f.a[3]) // 初始视图：旧链 A
	if err != nil {
		t.Fatal(err)
	}
	collector := indexer.NewLogCollector()
	store := indexer.NewStore()
	// 确认深度 1：h 需 tip-h>=1。tip=3 时仅 h<=2 落库（a1,a2 已确认，a3 未确认）。
	ix := indexer.New(indexer.Config{Confirmations: 1}, store, nil, collector.Logger())

	for h := uint64(0); h <= 3; h++ {
		if err := ix.Submit(f.blk(f.a[h])); err != nil {
			t.Fatal(err)
		}
	}
	if store.Count("old") != 2 {
		t.Fatalf("pre-reorg old count = %d, want 2 (a1,a2 confirmed; a3 below depth)", store.Count("old"))
	}
	// 深度 1 + tip 3：已确认 0,1,2；a3 未确认。
	if _, ok := store.HashAt(3); ok {
		t.Fatal("a3 must not be confirmed before tip advances beyond it")
	}
	oldH1, _ := store.HashAt(1)

	// 节点视图重组到新链 B（更高，tip=4）。
	if err := chain.SwitchTo(f.b[4]); err != nil {
		t.Fatal(err)
	}
	// 乱序投递新链 b3（与已确认 a3 高度相同但哈希不同）-> 触发重组判定。
	if err := ix.Submit(f.blk(f.b[3])); err != nil {
		t.Fatalf("conflicting block should be handled, not surfaced as fatal: %v", err)
	}
	// 此时 b1,b2 尚缺，分叉点可暂定在 3；补齐 b1 后应定位到真正分叉点 h=1。
	if err := ix.Submit(f.blk(f.b[1])); err != nil {
		t.Fatal(err)
	}
	// b1 与已确认 a1 同高度冲突，沿父指针 g 找到共同祖先 h=0 -> fork=1。
	// a1 先被撤销，b1 随即重放到 h=1，因此该处哈希必须已换成新链。
	h1Now, ok := store.HashAt(1)
	if !ok || h1Now != f.b[1] {
		t.Fatalf("h=1 must be rolled back and replayed onto b1, got ok=%v hash=%v", ok, h1Now)
	}
	if h1Now == oldH1 {
		t.Fatal("h=1 hash must differ from old chain a1")
	}
	if g0, ok := store.HashAt(0); !ok || g0 != f.g {
		t.Fatal("shared genesis h=0 must remain canonical")
	}
	// 再投 b2,b4 并上报新链头，驱动顺序归一与确认。
	if err := ix.Submit(f.blk(f.b[2])); err != nil {
		t.Fatal(err)
	}
	if err := ix.Submit(f.blk(f.b[4])); err != nil {
		t.Fatal(err)
	}
	ix.OfferTip(4)
	printLogs(t, collector, "重组回滚+重放流水")

	// 最终规范链必须是 g,b1,b2,b3,b4；h<=3 已确认（深度1），b4 未确认。
	want := map[uint64]indexer.Hash{
		0: f.b[0], 1: f.b[1], 2: f.b[2], 3: f.b[3],
	}
	for h, wantHash := range want {
		got, ok := store.HashAt(h)
		if !ok || got != wantHash {
			t.Fatalf("post-reorg canonical h=%d = %v, want %v", h, got, wantHash)
		}
	}
	if _, ok := store.HashAt(4); ok {
		t.Fatal("b4 must wait for confirmation depth")
	}
	// 派生计数收敛：旧链事件全部撤销，新链事件按确认数计入，绝不重复。
	if store.Count("old") != 0 {
		t.Fatalf("old-chain events must be fully revoked, got %d", store.Count("old"))
	}
	if store.Count("new") != 3 {
		t.Fatalf("new-chain confirmed events = %d, want 3 (b1,b2,b3)", store.Count("new"))
	}
	if oldH1 == f.b[1] {
		t.Fatal("sanity: old and new h1 hashes must differ")
	}
	// 流水必须包含可区分的回滚判定与 detach/attach。
	if !collector.Has(indexer.LogReorg, 1) {
		t.Fatal("must log reorg decision at fork h=1")
	}
	for h := uint64(1); h <= 2; h++ {
		if !collector.Has(indexer.LogDetach, h) {
			t.Fatalf("must log detach at h=%d", h)
		}
	}
	if !collector.Has(indexer.LogAttach, 1) || !collector.Has(indexer.LogAttach, 2) {
		t.Fatal("must log replayed attach at h=1,2")
	}
}

// TestReorgIdempotentReplay 验证抖动/重复投递不会让计数翻倍或丢数据。
func TestReorgIdempotentReplay(t *testing.T) {
	f := buildFork(t)
	chain, _ := f.builder.Build(f.a[2])
	collector := indexer.NewLogCollector()
	store := indexer.NewStore()
	ix := indexer.New(indexer.Config{Confirmations: 0}, store, nil, collector.Logger())

	for h := uint64(0); h <= 2; h++ {
		_ = ix.Submit(f.blk(f.a[h]))
	}
	if err := chain.SwitchTo(f.b[3]); err != nil {
		t.Fatal(err)
	}
	// 新链区块多轮乱序、重复投递（模拟短暂抖动）。
	for round := 0; round < 3; round++ {
		for _, hh := range []int{2, 1, 3, 2, 1} {
			_ = ix.Submit(f.blk(f.b[hh]))
		}
	}
	ix.OfferTip(3)
	printLogs(t, collector, "抖动/重复下的收敛流水")

	if store.Count("old") != 0 {
		t.Fatalf("old count must converge to 0, got %d", store.Count("old"))
	}
	if store.Count("new") != 3 {
		t.Fatalf("new count must converge to exactly 3, got %d", store.Count("new"))
	}
	if ix.NextHeight() != 4 {
		t.Fatalf("next = %d, want 4", ix.NextHeight())
	}
	if ix.PendingCount() != 0 {
		t.Fatalf("buffer must drain, pending=%d", ix.PendingCount())
	}
}
