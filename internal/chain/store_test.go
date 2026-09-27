package chain

import "testing"

func TestMemoryStoreCommitRollback(t *testing.T) {
	s := NewMemoryStore()
	b1 := Block{Height: 1, Hash: "h1", ParentHash: "genesis"}
	b2 := Block{Height: 2, Hash: "h2", ParentHash: "h1"}
	b3 := Block{Height: 3, Hash: "h3", ParentHash: "h2"}
	for _, b := range []Block{b1, b2, b3} {
		if err := s.Commit(b); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
	// 不连续落库必须报错
	if err := s.Commit(Block{Height: 5, Hash: "h5", ParentHash: "h3"}); err == nil {
		t.Fatal("高度不连续应报错")
	}
	// 父哈希错误必须报错
	if err := s.Commit(Block{Height: 4, Hash: "h4", ParentHash: "wrong"}); err == nil {
		t.Fatal("父哈希不匹配应报错")
	}
	removed, err := s.Rollback(2)
	if err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if len(removed) != 2 || removed[0].Hash != "h2" || removed[1].Hash != "h3" {
		t.Fatalf("回滚内容不符: %v", removed)
	}
	head, _ := s.Head()
	if head.Hash != "h1" || s.Len() != 1 {
		t.Fatalf("回滚后链头不符: %v len=%d", head, s.Len())
	}
}
