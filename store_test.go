package indexer_test

import (
	"testing"

	indexer "github.com/lechandonga/reorg-aware-chain-indexer-go"
)

func mkBlock(h uint64, parent indexer.Hash, key string) *indexer.Block {
	b := &indexer.Block{
		Height: h,
		Parent: parent,
		Events: []indexer.Event{{Key: key, Value: "v"}},
	}
	// 用 mock 的确定性哈希？为避免测试包依赖，手工赋值不同哈希即可。
	copy(b.Hash[:], []byte{byte(h + 1)})
	return b
}

func TestStoreContinuousPutAndDetach(t *testing.T) {
	s := indexer.NewStore()
	b0 := mkBlock(0, indexer.Hash{}, "k0")
	b1 := mkBlock(1, b0.Hash, "k1")
	b2 := mkBlock(2, b1.Hash, "k0") // k0 第二次出现

	if !s.PutConfirmed(b0) || !s.PutConfirmed(b1) || !s.PutConfirmed(b2) {
		t.Fatal("contiguous puts must succeed")
	}
	if s.Count("k0") != 2 || s.Count("k1") != 1 {
		t.Fatalf("counts wrong: %v", s.Counts())
	}
	// 非连续写入必须被拒绝。
	b9 := mkBlock(9, b2.Hash, "k9")
	if s.PutConfirmed(b9) {
		t.Fatal("non-contiguous put must be rejected")
	}
	// 回滚 h=2，k0 计数应回退到 1。
	if !s.Detach(2) {
		t.Fatal("detach tip must succeed")
	}
	if s.Count("k0") != 1 {
		t.Fatalf("after detach k0 count = %d, want 1", s.Count("k0"))
	}
	// 只能从 tip 摘。
	if s.Detach(0) {
		t.Fatal("detaching below tip must fail")
	}
}
