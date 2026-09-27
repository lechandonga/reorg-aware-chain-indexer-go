// Package indexer 实现乱序归一、确认深度与重组处理。
package indexer

import (
	"fmt"

	"github.com/lechandonga/reorg-aware-chain-indexer-go/internal/chain"
)

// Sequencer 把乱序/重复到达的区块按高度重排，缺口时报告等待原因。
type Sequencer struct {
	next   uint64                 // 下一个期望高度
	buffer map[uint64]chain.Block // 已到达但未到顺序的区块
	seen   map[string]uint64      // hash -> height，判重
}

func NewSequencer(startHeight uint64) *Sequencer {
	return &Sequencer{next: startHeight, buffer: map[uint64]chain.Block{}, seen: map[string]uint64{}}
}

// Push 接收一个区块；返回按序可消费的一批区块。
// 乱序先缓存，重复返回 ReasonDuplicate，缺口返回 ReasonGap。
func (s *Sequencer) Push(b chain.Block) ([]chain.Block, *chain.RejectError) {
	if h, ok := s.seen[b.Hash]; ok && h == b.Height {
		return nil, &chain.RejectError{Reason: chain.ReasonDuplicate, Height: b.Height,
			Detail: fmt.Sprintf("hash=%s 已在高度 %d 见过", b.Hash, h)}
	}
	if b.Height < s.next {
		// 高度已消费但 hash 没见过：可能是重组后的新分支，交给上层判定。
		return nil, &chain.RejectError{Reason: chain.ReasonConflict, Height: b.Height,
			Detail: fmt.Sprintf("高度 %d 已消费(next=%d)但 hash=%s 未知", b.Height, s.next, b.Hash)}
	}
	if b.Height > s.next {
		if _, ok := s.buffer[b.Height]; ok {
			return nil, &chain.RejectError{Reason: chain.ReasonDuplicate, Height: b.Height,
				Detail: "缓冲区已有同高度区块"}
		}
		s.buffer[b.Height] = b
		s.seen[b.Hash] = b.Height
		return nil, &chain.RejectError{Reason: chain.ReasonGap, Height: b.Height,
			Detail: fmt.Sprintf("期望高度 %d，收到 %d，先缓存等待", s.next, b.Height)}
	}
	// b.Height == s.next：连续吐出
	var out []chain.Block
	out = append(out, b)
	s.seen[b.Hash] = b.Height
	s.next++
	for {
		nb, ok := s.buffer[s.next]
		if !ok {
			break
		}
		delete(s.buffer, s.next)
		out = append(out, nb)
		s.next++
	}
	return out, nil
}

// Next 返回当前期望高度。
func (s *Sequencer) Next() uint64 { return s.next }

// Reset 在重组后重置期望高度，丢弃低于该高度的缓存与判重记录。
func (s *Sequencer) Reset(next uint64) {
	s.next = next
	for h := range s.buffer {
		if h < next {
			delete(s.buffer, h)
		}
	}
	for hash, h := range s.seen {
		if h < next {
			delete(s.seen, hash)
		}
	}
}
