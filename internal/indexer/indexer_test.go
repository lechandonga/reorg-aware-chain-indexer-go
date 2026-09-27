package indexer

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/lechandonga/reorg-aware-chain-indexer-go/internal/chain"
)

// makeChain 生成一条模拟链：hash 形如 "A-3"。
func makeChain(prefix string, from, to uint64, parentHash string) []chain.Block {
	var out []chain.Block
	parent := parentHash
	for h := from; h <= to; h++ {
		hash := fmt.Sprintf("%s-%d", prefix, h)
		out = append(out, chain.Block{Height: h, Hash: hash, ParentHash: parent,
			Payload: []string{fmt.Sprintf("tx-%s", hash)}})
		parent = hash
	}
	return out
}

func testLogger(t *testing.T) *log.Logger {
	t.Helper()
	return log.New(os.Stdout, "", log.Lmicroseconds)
}

// 收集 store 中所有 payload，验证不重复不遗漏。
func payloads(s chain.Store) []string {
	var out []string
	for h := 1; h <= s.Len(); h++ {
		b, _ := s.Get(uint64(h))
		out = append(out, b.Payload...)
	}
	return out
}

func TestOutOfOrderAndDuplicate(t *testing.T) {
	store := chain.NewMemoryStore()
	ix := New(store, 1, 2, testLogger(t))
	blocks := makeChain("A", 1, 6, "genesis")

	// 乱序投递：3,1,2,5,4（6 暂缺，制造缺口）
	order := []int{2, 0, 1, 4, 3}
	var gapSeen, dupSeen bool
	for _, i := range order {
		rej := ix.Ingest(blocks[i])
		if rej != nil {
			t.Logf("拒绝: %v", rej)
			if rej.Reason == chain.ReasonGap {
				gapSeen = true
			}
		}
	}
	if !gapSeen {
		t.Fatal("乱序投递应产生可区分的 gap 原因")
	}
	// 重复投递
	if rej := ix.Ingest(blocks[0]); rej == nil || rej.Reason != chain.ReasonDuplicate {
		t.Fatalf("重复投递应返回 duplicate, got %v", rej)
	}
	dupSeen = true
	// 补齐缺口
	if rej := ix.Ingest(blocks[5]); rej != nil {
		t.Fatalf("补齐缺口不应被拒绝: %v", rej)
	}
	ix.Finalize(8) // latest=8, depth=2 → 高度<=6 全部确认
	if store.Len() != 6 {
		t.Fatalf("期望落库 6 块, got %d", store.Len())
	}
	got := payloads(store)
	if len(got) != 6 {
		t.Fatalf("payload 数不符: %v", got)
	}
	for i, p := range got {
		want := fmt.Sprintf("tx-A-%d", i+1)
		if p != want {
			t.Fatalf("payload[%d]=%s want %s", i, p, want)
		}
	}
	if !dupSeen {
		t.Fatal("未观察到重复判定")
	}
}

func TestConfirmationDepth(t *testing.T) {
	store := chain.NewMemoryStore()
	ix := New(store, 1, 3, testLogger(t))
	for _, b := range makeChain("A", 1, 5, "genesis") {
		ix.Ingest(b)
	}
	ix.Finalize(5) // depth=3 → 只有高度<=2 可确认
	if store.Len() != 2 {
		t.Fatalf("确认深度未生效, 落库=%d want 2", store.Len())
	}
	ix.Finalize(8)
	if store.Len() != 5 {
		t.Fatalf("确认深度推进后落库=%d want 5", store.Len())
	}
}

func TestReorgConverges(t *testing.T) {
	store := chain.NewMemoryStore()
	ix := New(store, 1, 2, testLogger(t))
	oldChain := makeChain("A", 1, 6, "genesis")
	for _, b := range oldChain {
		ix.Ingest(b)
	}
	ix.Finalize(8) // 高度 1..6 全部落库
	if store.Len() != 6 {
		t.Fatalf("重组前落库=%d want 6", store.Len())
	}

	// 重组：从高度 4 开始分叉出新链 B（parent 为 A-3）
	newFork := makeChain("B", 4, 7, "A-3")
	// 新链块乱序到达；先于父块到达的会被标记 orphan 并缓存，父块到达后自动重放
	for _, i := range []int{1, 0, 3, 2} {
		rej := ix.Ingest(newFork[i])
		if rej != nil {
			t.Logf("暂存/拒绝: %v", rej)
			if rej.Reason != chain.ReasonOrphan && rej.Reason != chain.ReasonGap {
				t.Fatalf("重组块出现意外拒绝: %v", rej)
			}
		}
	}
	ix.Finalize(9) // depth=2 → 高度<=7 全部确认

	if store.Len() != 7 {
		t.Fatalf("重组后落库=%d want 7", store.Len())
	}
	// 验证收敛：1..3 仍是 A，4..7 是 B
	for h := 1; h <= 3; h++ {
		b, _ := store.Get(uint64(h))
		if b.Hash != fmt.Sprintf("A-%d", h) {
			t.Fatalf("高度 %d 应为 A 链, got %s", h, b.Hash)
		}
	}
	for h := 4; h <= 7; h++ {
		b, _ := store.Get(uint64(h))
		if b.Hash != fmt.Sprintf("B-%d", h) {
			t.Fatalf("高度 %d 应为 B 链, got %s", h, b.Hash)
		}
	}
	// 验证无重复计入：payload 总数 == 7
	if got := payloads(store); len(got) != 7 {
		t.Fatalf("重组后出现重复计入: %v", got)
	}
	// 链连续性
	for h := 2; h <= 7; h++ {
		cur, _ := store.Get(uint64(h))
		prev, _ := store.Get(uint64(h - 1))
		if cur.ParentHash != prev.Hash {
			t.Fatalf("高度 %d 父哈希断裂", h)
		}
	}
}

func TestReorgIdempotentReplay(t *testing.T) {
	// 短暂抖动：同一分叉重复投递两次，结果必须一致。
	store := chain.NewMemoryStore()
	ix := New(store, 1, 1, testLogger(t))
	for _, b := range makeChain("A", 1, 4, "genesis") {
		ix.Ingest(b)
	}
	ix.Finalize(5)
	fork := makeChain("B", 3, 5, "A-2")
	for round := 0; round < 2; round++ {
		for _, b := range fork {
			ix.Ingest(b)
		}
		ix.Finalize(6)
	}
	if store.Len() != 5 {
		t.Fatalf("抖动后落库=%d want 5", store.Len())
	}
	if got := payloads(store); len(got) != 5 {
		t.Fatalf("抖动导致重复计入: %v", got)
	}
	b3, _ := store.Get(3)
	if b3.Hash != "B-3" {
		t.Fatalf("高度 3 应收敛到 B 链, got %s", b3.Hash)
	}
}

func TestOrphanRejected(t *testing.T) {
	store := chain.NewMemoryStore()
	ix := New(store, 1, 1, testLogger(t))
	for _, b := range makeChain("A", 1, 3, "genesis") {
		ix.Ingest(b)
	}
	ix.Finalize(4)
	// 一个父哈希完全不认识的低高度块 → 孤儿拒绝
	bad := chain.Block{Height: 2, Hash: "X-2", ParentHash: "nobody"}
	rej := ix.Ingest(bad)
	if rej == nil || rej.Reason != chain.ReasonOrphan {
		t.Fatalf("期望 orphan 拒绝, got %v", rej)
	}
	t.Logf("拒绝原因: %v", rej)
}
