package mock

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"

	indexer "github.com/lechandonga/reorg-aware-chain-indexer-go"
)

// 预定义错误：indexer 包通过 errors.Is 区分。
var (
	// ErrNotFound 表示高度超过当前链头。
	ErrNotFound = errors.New("mock: block not found")
	// ErrNodeUnavailable 表示节点超时/宕机/抖动。
	ErrNodeUnavailable = errors.New("mock: node unavailable")
	// ErrUnknownTip 表示 SwitchTo 指向了一个未构建的区块。
	ErrUnknownTip = errors.New("mock: unknown tip block")
)

// Chain 是可重组的内存链。
// 区块图中可以同时存在共同前缀 + 多条侧链；
// SwitchTo 之后，BlockAt/Tip 的“规范视图”立即切换到侧链。
type Chain struct {
	mu sync.RWMutex
	// blocks 保存图中所有区块（含侧链），按哈希索引。
	blocks map[indexer.Hash]*indexer.Block
	// canonical[h] 是当前规范链在高度 h 上的哈希。
	canonical []indexer.Hash
	tip       indexer.Hash
}

// blockStore 给 Builder 和 Chain 共用的底层图。
// 这里直接让 Builder 产出 map 后交给 Chain。

// Builder 按“高度 + 父哈希 + 事件内容”确定性地生成哈希，
// 因此两条链在共同前缀上的区块哈希天然一致，分叉后才不同。
type Builder struct {
	blocks map[indexer.Hash]*indexer.Block
}

// NewBuilder 创建建链器。
func NewBuilder() *Builder {
	return &Builder{blocks: make(map[indexer.Hash]*indexer.Block)}
}

// ComputeHash 根据区块内容确定性计算哈希（Builder 与测试均可调用）。
func ComputeHash(height uint64, parent indexer.Hash, evs []indexer.Event) indexer.Hash {
	h := sha256.New()
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], height)
	h.Write(buf[:])
	h.Write(parent[:])
	for _, e := range evs {
		h.Write([]byte(e.Key))
		h.Write([]byte{0})
		h.Write([]byte(e.Value))
		h.Write([]byte{0})
	}
	var out indexer.Hash
	copy(out[:], h.Sum(nil))
	return out
}

// Add 向图中添加一个区块，返回其哈希。
// height 必须等于父区块高度 + 1（genesis 高度为 0，父为零哈希）。
// 重复添加相同内容的区块是幂等的（返回同一哈希）——用于模拟重复投递。
func (b *Builder) Add(height uint64, parent indexer.Hash, evs []indexer.Event) indexer.Hash {
	hash := ComputeHash(height, parent, evs)
	if _, ok := b.blocks[hash]; !ok {
		if height > 0 {
			if _, ok := b.blocks[parent]; !ok {
				panic("mock: parent block not built yet")
			}
		}
		cp := make([]indexer.Event, len(evs))
		copy(cp, evs)
		b.blocks[hash] = &indexer.Block{
			Height: height,
			Hash:   hash,
			Parent: parent,
			Events: cp,
		}
	}
	return hash
}

// Block 返回已构建区块（不存在返回 nil），便于测试搭建侧链。
func (b *Builder) Block(hash indexer.Hash) *indexer.Block {
	blk := b.blocks[hash]
	return blk
}

// Build 以 tip 为链头产出一条可重组链，tip 必须是 Add 过的区块。
func (b *Builder) Build(tip indexer.Hash) (*Chain, error) {
	if _, ok := b.blocks[tip]; !ok {
		return nil, ErrUnknownTip
	}
	c := &Chain{blocks: make(map[indexer.Hash]*indexer.Block, len(b.blocks))}
	for h, blk := range b.blocks {
		cp := *blk
		c.blocks[h] = &cp
	}
	if err := c.setTip(tip); err != nil {
		return nil, err
	}
	return c, nil
}

// setTip 从 tip 沿父哈希回溯到 genesis，重建规范链切片。
func (c *Chain) setTip(tip indexer.Hash) error {
	var path []indexer.Hash
	cur := tip
	for {
		blk, ok := c.blocks[cur]
		if !ok {
			return ErrUnknownTip
		}
		path = append(path, cur)
		if blk.Height == 0 {
			break
		}
		cur = blk.Parent
	}
	// path 从高到低，反转成按高度索引。
	canonical := make([]indexer.Hash, len(path))
	for i, h := range path {
		canonical[len(path)-1-i] = h
	}
	c.canonical = canonical
	c.tip = tip
	return nil
}

// SwitchTo 让规范视图重组到以 sideTip 为链头的链。
// sideTip 必须与当前链共享某段前缀（即区块图里存在该块）。
func (c *Chain) SwitchTo(sideTip indexer.Hash) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.setTip(sideTip)
}

// BlockAtHeight 返回当前规范视图中指定高度的区块。
func (c *Chain) BlockAtHeight(height uint64) (*indexer.Block, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if height >= uint64(len(c.canonical)) {
		return nil, false
	}
	blk := c.blocks[c.canonical[height]]
	cp := *blk
	return &cp, true
}

// HashAt 返回当前规范视图指定高度的哈希（测试断言用）。
func (c *Chain) HashAt(height uint64) (indexer.Hash, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if height >= uint64(len(c.canonical)) {
		return indexer.Hash{}, false
	}
	return c.canonical[height], true
}

// Tip 返回当前规范视图的链头高度。
func (c *Chain) Tip() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return uint64(len(c.canonical)) - 1
}

// Append 在当前规范链尾部追加一个新区块并返回其哈希。
// 用于“链持续增长”的长时间运行测试。
func (c *Chain) Append(evs []indexer.Event) indexer.Hash {
	c.mu.Lock()
	defer c.mu.Unlock()
	height := uint64(len(c.canonical))
	parent := c.tip
	hash := ComputeHash(height, parent, evs)
	if _, exists := c.blocks[hash]; !exists {
		cp := make([]indexer.Event, len(evs))
		copy(cp, evs)
		c.blocks[hash] = &indexer.Block{
			Height: height, Hash: hash, Parent: parent, Events: cp,
		}
	}
	c.canonical = append(c.canonical, hash)
	c.tip = hash
	return hash
}

// AppendBlockAt 是 Append 的便捷封装（与历史命名保持一致的测试辅助）。
func (c *Chain) AppendBlockAt(evs []indexer.Event) indexer.Hash { return c.Append(evs) }
