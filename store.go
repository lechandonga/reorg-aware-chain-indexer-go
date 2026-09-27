package indexer

import "sync"

// Store 是索引结果的内存持久化抽象。
// 只有达到确认深度的区块才允许通过 PutConfirmed 写入；
// 重组时按高度从高到低 Detach，再 PutConfirmed 新链区块，
// 派生计数 counts 随事件增减，最终收敛到唯一正确结论。
type Store struct {
	mu sync.RWMutex
	// canonical 按高度保存已确认区块（连续、无缺口）。
	canonical map[uint64]*Block
	// counts 保存事件 Key 的累计计数（同一区块重放只计一次）。
	counts map[string]int
	// tip 为最高已确认高度；空库时 tipSet=false。
	tip    uint64
	tipSet bool
}

// NewStore 创建一个空存储。
func NewStore() *Store {
	return &Store{
		canonical: make(map[uint64]*Block),
		counts:    make(map[string]int),
	}
}

// PutConfirmed 正式落库一个已确认区块。
// 约束：必须按高度连续写入（h == tip+1 或空库首块）。
// 返回 false 表示违反连续性或与已存区块哈希重复/冲突（调用方应先 Detach）。
func (s *Store) PutConfirmed(b *Block) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.canonical[b.Height]; ok {
		return existing.Hash == b.Hash // 幂等：完全相同才视为成功
	}
	if s.tipSet && b.Height != s.tip+1 {
		return false
	}
	// 链式校验：非首块必须正确指向前一已确认区块，
	// 防止重组期间残留的旧链区块被错误接到新链上。
	if b.Height > 0 {
		if prev, ok := s.canonical[b.Height-1]; ok && b.Parent != prev.Hash {
			return false
		}
	}
	if !s.tipSet {
		s.tipSet = true
		s.tip = b.Height - 1 // 首块可能从 StartHeight 开始
		if b.Height == 0 {
			s.tip = 0
		}
	}
	cp := *b
	cp.Events = append([]Event(nil), b.Events...)
	s.canonical[b.Height] = &cp
	for _, e := range cp.Events {
		s.counts[e.Key]++
	}
	s.tip = b.Height
	return true
}

// Detach 撤销最高已确认区块（必须等于当前 tip），并回退其派生计数。
// 重组回滚时由调用方从 tip 向 forkHeight 逐块调用。
func (s *Store) Detach(height uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	blk, ok := s.canonical[height]
	if !ok || (s.tipSet && height != s.tip) {
		return false
	}
	for _, e := range blk.Events {
		s.counts[e.Key]--
		if s.counts[e.Key] == 0 {
			delete(s.counts, e.Key)
		}
	}
	delete(s.canonical, height)
	if height == 0 || len(s.canonical) == 0 {
		s.tip = 0
		s.tipSet = false
		return true
	}
	s.tip = height - 1
	return true
}

// HashAt 返回已确认高度上的区块哈希。
func (s *Store) HashAt(height uint64) (Hash, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	blk, ok := s.canonical[height]
	if !ok {
		return Hash{}, false
	}
	return blk.Hash, true
}

// BlockAt 返回已确认区块的副本（不存在返回 nil）。
func (s *Store) BlockAt(height uint64) *Block {
	s.mu.RLock()
	defer s.mu.RUnlock()
	blk, ok := s.canonical[height]
	if !ok {
		return nil
	}
	cp := *blk
	cp.Events = append([]Event(nil), blk.Events...)
	return &cp
}

// Tip 返回最高已确认高度；空库时 ok=false。
func (s *Store) Tip() (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tip, s.tipSet
}

// Count 返回某个事件 Key 的累计计数。
func (s *Store) Count(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.counts[key]
}

// Counts 返回派生计数的只读快照（测试断言最终收敛结果）。
func (s *Store) Counts() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]int, len(s.counts))
	for k, v := range s.counts {
		out[k] = v
	}
	return out
}

// CanonicalHashes 返回 [0,tip] 上已确认哈希的快照（按高度排列，供流水打印）。
func (s *Store) CanonicalHashes() map[uint64]Hash {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[uint64]Hash, len(s.canonical))
	for h, blk := range s.canonical {
		out[h] = blk.Hash
	}
	return out
}
