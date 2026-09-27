package chain

import "fmt"

// Store 是已确认区块的存储接口。
type Store interface {
	// Commit 落库一个已确认区块（追加到规范链）。
	Commit(b Block) error
	// Rollback 撤销 height 及其以上的所有已落库区块，返回被撤销的区块（按高度升序）。
	Rollback(height uint64) ([]Block, error)
	// Head 返回当前规范链头；空链时 ok=false。
	Head() (Block, bool)
	// Get 按高度取已落库区块。
	Get(height uint64) (Block, bool)
	// Len 返回已落库区块数。
	Len() int
}

// MemoryStore 是 Store 的内存实现，假定区块高度从 1 开始连续。
type MemoryStore struct {
	blocks []Block
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

func (s *MemoryStore) Commit(b Block) error {
	if head, ok := s.Head(); ok {
		if b.Height != head.Height+1 {
			return fmt.Errorf("store: 落库高度不连续 head=%d got=%d", head.Height, b.Height)
		}
		if b.ParentHash != head.Hash {
			return fmt.Errorf("store: 父哈希不匹配 head=%s parent=%s", head.Hash, b.ParentHash)
		}
	} else if b.Height != 1 {
		return fmt.Errorf("store: 首块高度必须为 1, got=%d", b.Height)
	}
	s.blocks = append(s.blocks, b)
	return nil
}

func (s *MemoryStore) Rollback(height uint64) ([]Block, error) {
	if height == 0 || int(height) > len(s.blocks) {
		return nil, fmt.Errorf("store: 回滚高度越界 %d (len=%d)", height, len(s.blocks))
	}
	removed := make([]Block, len(s.blocks)-int(height)+1)
	copy(removed, s.blocks[height-1:])
	s.blocks = s.blocks[:height-1]
	return removed, nil
}

func (s *MemoryStore) Head() (Block, bool) {
	if len(s.blocks) == 0 {
		return Block{}, false
	}
	return s.blocks[len(s.blocks)-1], true
}

func (s *MemoryStore) Get(height uint64) (Block, bool) {
	if height == 0 || int(height) > len(s.blocks) {
		return Block{}, false
	}
	return s.blocks[height-1], true
}

func (s *MemoryStore) Len() int { return len(s.blocks) }
