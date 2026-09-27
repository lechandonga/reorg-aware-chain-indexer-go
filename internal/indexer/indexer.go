package indexer

import (
	"log"

	"github.com/lechandonga/reorg-aware-chain-indexer-go/internal/chain"
)

// Indexer 消费按序区块，执行确认深度控制与重组处理。
type Indexer struct {
	store        chain.Store
	seq          *Sequencer
	confirmDepth uint64                 // 确认深度：latestHeight - h >= confirmDepth 才落库
	pending      []chain.Block          // 已按序但未确认的区块
	orphans      map[string]chain.Block // 父块未到的疑似重组块，等待重放
	draining     bool
	logger       *log.Logger
}

func New(store chain.Store, startHeight, confirmDepth uint64, logger *log.Logger) *Indexer {
	if logger == nil {
		logger = log.Default()
	}
	return &Indexer{store: store, seq: NewSequencer(startHeight), confirmDepth: confirmDepth,
		orphans: map[string]chain.Block{}, logger: logger}
}

// hashAt 返回指定高度已知的 hash（先查 pending，再查已落库）。
func (ix *Indexer) hashAt(height uint64) (string, bool) {
	if len(ix.pending) > 0 {
		base := ix.pending[0].Height
		if height >= base && int(height-base) < len(ix.pending) {
			return ix.pending[height-base].Hash, true
		}
	}
	if b, ok := ix.store.Get(height); ok {
		return b.Hash, true
	}
	return "", false
}

// Ingest 接收一个可能乱序/重复的区块；返回 nil 表示被接受（落序或缓存）。
func (ix *Indexer) Ingest(b chain.Block) *chain.RejectError {
	out, rej := ix.seq.Push(b)
	if rej != nil {
		if rej.Reason == chain.ReasonConflict {
			// 已消费高度出现未知 hash：判定是否构成重组。
			rj := ix.tryReorg(b)
			if rj == nil {
				ix.drainOrphans()
				return nil
			}
			if rj.Reason == chain.ReasonOrphan {
				// 父块可能乱序未到，先缓存，等重组点出现时重放。
				ix.orphans[b.Hash] = b
			}
			return rj
		}
		ix.logger.Printf("[ingest] 拒绝 height=%d hash=%s 原因=%s", b.Height, b.Hash, rej.Reason)
		return rej
	}
	for _, blk := range out {
		ix.logger.Printf("[ingest] 高度流水 +%d hash=%s parent=%s", blk.Height, blk.Hash, blk.ParentHash)
		ix.pending = append(ix.pending, blk)
	}
	ix.drainOrphans()
	return nil
}

// drainOrphans 重放孤儿缓冲：链每前进一步都重试，直到没有进展。
func (ix *Indexer) drainOrphans() {
	if ix.draining {
		return
	}
	ix.draining = true
	defer func() { ix.draining = false }()
	for len(ix.orphans) > 0 {
		progress := false
		for hash, b := range ix.orphans {
			rej := ix.Ingest(b)
			if rej == nil || rej.Reason != chain.ReasonOrphan {
				delete(ix.orphans, hash)
				progress = true
			}
		}
		if !progress {
			return
		}
	}
}

// tryReorg 处理重组：若 b 的父哈希与已知链在 b.Height-1 处吻合，
// 则撤销该高度以上的 pending 与已落库区块，重置定序器并重放。
func (ix *Indexer) tryReorg(b chain.Block) *chain.RejectError {
	if b.Height == 0 {
		return &chain.RejectError{Reason: chain.ReasonOrphan, Height: b.Height, Detail: "高度为 0"}
	}
	parent, ok := ix.hashAt(b.Height - 1)
	if !ok || parent != b.ParentHash {
		known := "无"
		if ok {
			known = parent
		}
		ix.logger.Printf("[reorg] 判定失败 height=%d 期望parent=%s 已知=%s → 孤儿块拒绝",
			b.Height, b.ParentHash, known)
		return &chain.RejectError{Reason: chain.ReasonOrphan, Height: b.Height,
			Detail: "父哈希与已知链不吻合，无法定位分叉点"}
	}
	fork := b.Height - 1
	// 截断 pending
	kept := 0
	if len(ix.pending) > 0 {
		base := ix.pending[0].Height
		if fork >= base {
			kept = int(fork - base + 1)
		}
	}
	dropped := len(ix.pending) - kept
	ix.pending = ix.pending[:kept]
	// 回滚已落库部分
	var rolled []chain.Block
	if head, ok := ix.store.Head(); ok && head.Height > fork {
		var err error
		rolled, err = ix.store.Rollback(fork + 1)
		if err != nil {
			ix.logger.Printf("[reorg] 回滚失败: %v", err)
			return &chain.RejectError{Reason: chain.ReasonOrphan, Height: b.Height, Detail: err.Error()}
		}
	}
	ix.logger.Printf("[reorg] 判定依据: 高度 %d 处 parent=%s 与已知链吻合，分叉点=%d，撤销 pending=%d 已落库=%d",
		b.Height, b.ParentHash, fork, dropped, len(rolled))
	ix.seq.Reset(b.Height)
	out, rej := ix.seq.Push(b)
	if rej != nil {
		return rej
	}
	// 重放：被撤销的已落库区块若仍在新链上会由后续拉取重新到达；
	// 这里把被撤销的块重新入队尝试（若与新链一致会被接受，否则以可区分原因拒绝）。
	for _, blk := range out {
		ix.logger.Printf("[reorg] 高度流水 +%d hash=%s (新分支)", blk.Height, blk.Hash)
		ix.pending = append(ix.pending, blk)
	}
	for _, old := range rolled {
		if rej := ix.Ingest(old); rej != nil {
			ix.logger.Printf("[reorg] 旧链块 height=%d hash=%s 不再适用: %s", old.Height, old.Hash, rej.Reason)
		}
	}
	return nil
}

// Finalize 根据最新链头高度，把达到确认深度的 pending 区块落库。
// 已落库的同高度同 hash 区块会被幂等跳过，避免抖动导致重复计入。
func (ix *Indexer) Finalize(latestHeight uint64) {
	i := 0
	for i < len(ix.pending) {
		b := ix.pending[i]
		if latestHeight < b.Height+ix.confirmDepth {
			ix.logger.Printf("[finalize] 高度 %d 未达确认深度(latest=%d depth=%d)，等待",
				b.Height, latestHeight, ix.confirmDepth)
			break
		}
		if existing, ok := ix.store.Get(b.Height); ok {
			if existing.Hash == b.Hash {
				ix.logger.Printf("[finalize] 高度 %d 已落库且一致，幂等跳过", b.Height)
				i++
				continue
			}
			// 已落库区块与新链冲突：先回滚再落库（兜底，正常路径在 Ingest 已处理）。
			ix.logger.Printf("[reorg] 落库时发现冲突 height=%d old=%s new=%s，回滚重放",
				b.Height, existing.Hash, b.Hash)
			if _, err := ix.store.Rollback(b.Height); err != nil {
				ix.logger.Printf("[reorg] 回滚失败: %v", err)
				return
			}
		}
		if err := ix.store.Commit(b); err != nil {
			ix.logger.Printf("[finalize] 落库失败 height=%d: %v", b.Height, err)
			return
		}
		ix.logger.Printf("[finalize] 落库 height=%d hash=%s", b.Height, b.Hash)
		i++
	}
	ix.pending = ix.pending[i:]
}

// Store 返回底层存储（只读用途）。
func (ix *Indexer) Store() chain.Store { return ix.store }
