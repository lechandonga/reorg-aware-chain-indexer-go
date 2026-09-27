package indexer_test

import (
	"errors"
	"testing"

	indexer "github.com/lechandonga/reorg-aware-chain-indexer-go"
	mock "github.com/lechandonga/reorg-aware-chain-indexer-go/mock"
)

// printLogs 把高度流水打印到测试日志（需求要求单测日志可见）。
func printLogs(t *testing.T, c *indexer.LogCollector, title string) {
	t.Helper()
	t.Logf("===== %s =====", title)
	c.Print(t.Logf)
}

// linearBlocks 用 mock.Builder 构建线性链并返回每个高度的区块副本。
func linearBlocks(t *testing.T, n int, prefix string) (*mock.Chain, []*indexer.Block) {
	t.Helper()
	b := mock.NewBuilder()
	var prev indexer.Hash
	blks := make([]*indexer.Block, n+1)
	for h := 0; h <= n; h++ {
		evs := []indexer.Event{{Key: prefix + ":tx", Value: "h" + heightStr(uint64(h))}}
		hash := b.Add(uint64(h), prev, evs)
		blk := b.Block(hash)
		if blk == nil {
			t.Fatalf("block %d missing in builder", h)
		}
		blks[h] = blk
		prev = hash
	}
	c, err := b.Build(prev)
	if err != nil {
		t.Fatal(err)
	}
	return c, blks
}

func heightStr(h uint64) string {
	if h == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for h > 0 {
		i--
		buf[i] = byte('0' + h%10)
		h /= 10
	}
	return string(buf[i:])
}

func TestOutOfOrderAndGap(t *testing.T) {
	_, blks := linearBlocks(t, 5, "lin")
	collector := indexer.NewLogCollector()
	ix := indexer.New(indexer.Config{Confirmations: 0}, indexer.NewStore(), nil, collector.Logger())

	// 先投递 h=2（缺口 0、1 在前）-> 必须给出可区分的 gap_ahead 等待原因。
	if err := ix.Submit(blks[2]); !errors.Is(err, indexer.ErrGapAhead) {
		t.Fatalf("h=2 before gap, want ErrGapAhead, got %v", err)
	}
	if ix.PendingCount() != 1 {
		t.Fatalf("h=2 should wait in buffer, pending=%d", ix.PendingCount())
	}
	// 直接跳投 h=4 -> 仍因缺口等待，绝不能静默跳过 h=0,1,3。
	if err := ix.Submit(blks[4]); !errors.Is(err, indexer.ErrGapAhead) {
		t.Fatalf("h=4, want ErrGapAhead, got %v", err)
	}
	// 补齐 h=0,1，h=2 自动顺序归一；h=4 仍等待 h=3。
	if err := ix.Submit(blks[0]); err != nil {
		t.Fatal(err)
	}
	if err := ix.Submit(blks[1]); err != nil {
		t.Fatal(err)
	}
	// 再投递 h=3 应带动 h=2..4 顺序归一落库；最后补 h=5。
	if err := ix.Submit(blks[3]); err != nil {
		t.Fatal(err)
	}
	// h=4 之前在缓冲，h=3 到达后应自动推进；再补 h=5。
	if err := ix.Submit(blks[5]); err != nil {
		t.Fatal(err)
	}
	printLogs(t, collector, "乱序+缺口流水")

	reasons := collector.Reasons()
	var sawGap bool
	for _, r := range reasons {
		if r == indexer.ReasonGapAhead {
			sawGap = true
		}
	}
	if !sawGap {
		t.Fatal("must record gap_ahead reasons")
	}
	if ix.NextHeight() != 6 {
		t.Fatalf("next = %d, want 6 (all blocks committed in order)", ix.NextHeight())
	}
	if ix.PendingCount() != 0 {
		t.Fatalf("buffer must drain, pending=%d", ix.PendingCount())
	}
}

func TestDuplicateDelivery(t *testing.T) {
	_, blks := linearBlocks(t, 3, "dup")
	collector := indexer.NewLogCollector()
	ix := indexer.New(indexer.Config{Confirmations: 0}, indexer.NewStore(), nil, collector.Logger())

	for _, h := range []uint64{0, 1, 2} {
		if err := ix.Submit(blks[h]); err != nil {
			t.Fatal(err)
		}
	}
	// 已落库区块重复投递 -> ErrAlreadyCanonical，计数不能重复增加。
	if err := ix.Submit(blks[1]); !errors.Is(err, indexer.ErrAlreadyCanonical) {
		t.Fatalf("canonical dup, want ErrAlreadyCanonical, got %v", err)
	}
	// 乱序重复（缓冲中）-> ErrAlreadyBuffered。
	ix2 := indexer.New(indexer.Config{Confirmations: 0}, indexer.NewStore(), nil, collector.Logger())
	_ = ix2.Submit(blks[2]) // 缺口等待，进入缓冲
	if err := ix2.Submit(blks[2]); !errors.Is(err, indexer.ErrAlreadyBuffered) {
		t.Fatalf("buffered dup, want ErrAlreadyBuffered, got %v", err)
	}
	printLogs(t, collector, "重复投递流水")
}
