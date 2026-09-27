package indexer

import "fmt"

// reorg.go: 重组（链回滚）的判定与执行。
//
// 判定依据（会写进 LogReorg 的 Detail，测试日志可见）：
//  1. 同一高度出现与“已确认链”或“缓冲中同高度区块”不同的哈希；
//  2. 从冲突高度沿新区块的 Parent 指针逐格回溯，与已知链逐高度比对，
//     找到第一个哈希一致的高度 —— 其上方即为分叉点；
//  3. 找不到共同祖先（完全不同的链）时，分叉点为 0 并在依据中注明。

// chainLookup 按高度返回当前“已知链”（优先未确认缓冲，其次已确认存储）的区块。
type chainLookup func(height uint64) (*Block, bool)

// reorgDecision 描述一次重组判定结果。
type reorgDecision struct {
	Detected   bool
	ForkHeight uint64 // 从该高度（含）起回滚
	OldHash    Hash   // 分叉高度上旧链的哈希
	NewHash    Hash   // 分叉高度上新链的哈希
	Reason     string // 人类可读的判定依据
}

// String 输出判定依据摘要。
func (d reorgDecision) String() string { return d.Reason }

// detectReorg 在 height 处发现哈希冲突后，沿父哈希回溯定位分叉点。
// lookup 同时覆盖 pending 缓冲与 Store；incoming 为冲突高度上的新区块。
func detectReorg(lookup chainLookup, incoming *Block) reorgDecision {
	h := incoming.Height
	newHash := incoming.Hash
	// 旧链在该高度的哈希（缓冲或已落库）。
	oldBlk, ok := lookup(h)
	if !ok {
		// 该高度尚无旧区块，不构成重组（只是乱序到达的新块）。
		return reorgDecision{Detected: false}
	}
	if oldBlk.Hash == newHash {
		return reorgDecision{Detected: false}
	}
	oldAtConflict := oldBlk.Hash

	// 逐格沿父指针回溯：cur 为“新链在 height=curH 上的区块”。
	cur := incoming
	curH := h
	for curH > 0 {
		prevH := curH - 1
		prevKnown, ok := lookup(prevH)
		if !ok {
			// 新链在 prevH 的父哈希未知（缓冲里缺块），无法继续判定，
			// 视为需要拉取更多区块后再判：报告冲突但分叉点暂定为 curH。
			return reorgDecision{
				Detected:   true,
				ForkHeight: curH,
				OldHash:    oldHashAt(lookup, curH),
				NewHash:    cur.Hash,
				Reason: fmt.Sprintf(
					"hash conflict at h=%d (old=%s new=%s); parent view missing below h=%d, tentative fork=%d",
					h, shortHash(oldAtConflict), shortHash(newHash), curH, curH),
			}
		}
		if prevKnown.Hash == cur.Parent {
			// 共同祖先 = prevH；分叉从 curH 开始。
			return reorgDecision{
				Detected:   true,
				ForkHeight: curH,
				OldHash:    oldHashAt(lookup, curH),
				NewHash:    cur.Hash,
				Reason: fmt.Sprintf(
					"hash conflict at h=%d (old=%s new=%s); common ancestor h=%d matches %s, rollback from h=%d",
					h, shortHash(oldAtConflict), shortHash(newHash), prevH, shortHash(cur.Parent), curH),
			}
		}
		// 继续沿新链向上找父区块（必须已在缓冲中，否则无法继续）。
		parent, ok := lookupBlockByHash(lookup, cur.Parent, prevH)
		if !ok {
			return reorgDecision{
				Detected:   true,
				ForkHeight: curH,
				OldHash:    oldHashAt(lookup, curH),
				NewHash:    cur.Hash,
				Reason: fmt.Sprintf(
					"hash conflict at h=%d (old=%s new=%s); new-parent %s absent at h=%d, tentative fork=%d",
					h, shortHash(oldAtConflict), shortHash(newHash), shortHash(cur.Parent), prevH, curH),
			}
		}
		cur = parent
		curH = prevH
	}
	// curH == 0 仍不一致：两条链没有共同祖先。
	return reorgDecision{
		Detected:   true,
		ForkHeight: 0,
		OldHash:    oldHashAt(lookup, 0),
		NewHash:    cur.Hash,
		Reason: fmt.Sprintf(
			"hash conflict at h=%d (old=%s new=%s); no common ancestor at genesis, rollback from h=0",
			h, shortHash(oldAtConflict), shortHash(newHash)),
	}
}

// oldHashAt 安全读取某高度旧链哈希，缺失时返回零哈希。
func oldHashAt(lookup chainLookup, height uint64) Hash {
	if blk, ok := lookup(height); ok {
		return blk.Hash
	}
	return Hash{}
}

// lookupBlockByHash 在指定高度按哈希查找新区块（用于沿父指针回溯）。
func lookupBlockByHash(lookup chainLookup, hash Hash, height uint64) (*Block, bool) {
	blk, ok := lookup(height)
	if !ok || blk.Hash != hash {
		return nil, false
	}
	return blk, true
}

// applyRollback 把 Store 上 [fork, tip] 的已确认区块逐块撤销。
// 每撤销一块记录一条 LogDetach 流水。
func applyRollback(store *Store, fork, tip uint64, log func(LogEntry)) {
	for h := tip; h >= fork; h-- {
		blk := store.BlockAt(h)
		if !store.Detach(h) {
			// 理论上不会发生；防御性停止以避免下溢。
			break
		}
		h2 := h
		detail := ""
		if blk != nil {
			detail = fmt.Sprintf("parent=%s", shortHash(blk.Parent))
		}
		log(LogEntry{Kind: LogDetach, Height: h2, Hash: hashOf(blk), Detail: detail})
		if h == 0 {
			break
		}
	}
}

func hashOf(b *Block) Hash {
	if b == nil {
		return Hash{}
	}
	return b.Hash
}
