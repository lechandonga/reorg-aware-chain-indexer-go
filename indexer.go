package indexer

import (
	"context"
	"sort"
	"sync"
	"time"
)

// indexer.go: 拉取 -> 顺序归一 -> 确认 -> 落库 -> 重组收敛 的总装。
//
// 线程模型：
//   - “取数”可以多节点/多 worker 并发（见 fetcher.go）；
//   - 所有索引状态（pending / next / Store）只在 Submit->advance 这条
//     调用链上被修改，Run 循环拿到区块后也按高度排序、逐个串行 Submit，
//     因此不存在乱序落库与数据竞争；一把 mutex 只保护停止信号。

// defaultRunInterval 是后台拉取循环的轮询间隔。
const defaultRunInterval = 25 * time.Millisecond

// maxBuffered 限制乱序缓冲容量：超出窗口的过远区块按 gap_ahead 拒绝，
// 保证异常投递（无限乱序/恶意高高度）不会让内存无限增长。
const maxBuffered = 4096

// Config 是索引器配置。
type Config struct {
	// Confirmations 为确认深度：仅当 tip-height >= Confirmations 时区块才落库。
	Confirmations uint64
	// MaxInFlight 限制并发取数 worker 数与单批拉取大小（内存有界）。
	MaxInFlight int
	// StartHeight 为空库时的起始高度。
	StartHeight uint64
	// RunInterval 为后台轮询间隔，0 使用默认值。
	RunInterval time.Duration
}

// Indexer 串起拉取、缓冲、确认判定、落库与重组处理。
type Indexer struct {
	cfg     Config
	store   *Store
	clients []Client
	logf    func(LogEntry)

	stopMu  sync.Mutex
	stopped bool
	cancel  context.CancelFunc
	done    chan struct{}

	// stateMu 保护下面所有索引状态。设计上写者只有后台循环一个 goroutine，
	// 但 Submit/OfferTip 也可能被外部在运行期间调用，且测试会并发读取
	// NextHeight/PendingCount，因此统一加锁，杜绝数据竞争。
	stateMu sync.Mutex
	pending map[uint64]*Block
	next    uint64
	tip     uint64
	tipSet  bool
	// 上一次重组需要重放的高度区间，区间内的落库记 LogAttach 而非 LogConfirm。
	// reorgTo 取“旧链实际顶端”：确认区块从该高度回滚，新链就重放到该高度；
	// 之后（新链更长的部分）属于普通前向确认。
	reorgFrom, reorgTo uint64
	hasReorgSpan       bool
}

// New 创建索引器。store 中已有数据时从 store tip+1 继续。
// logf 为 nil 时丢弃流水（测试可传 LogCollector.Logger()）。
func New(cfg Config, store *Store, clients []Client, logf func(LogEntry)) *Indexer {
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 4
	}
	if cfg.RunInterval <= 0 {
		cfg.RunInterval = defaultRunInterval
	}
	if logf == nil {
		logf = func(LogEntry) {}
	}
	ix := &Indexer{
		cfg:     cfg,
		store:   store,
		clients: clients,
		logf:    logf,
		pending: make(map[uint64]*Block),
		next:    cfg.StartHeight,
		done:    make(chan struct{}),
	}
	if tip, ok := store.Tip(); ok {
		ix.next = tip + 1
	}
	return ix
}

// NextHeight 返回下一个期望落库高度（测试/诊断用）。
func (ix *Indexer) NextHeight() uint64 {
	ix.stateMu.Lock()
	defer ix.stateMu.Unlock()
	return ix.next
}

// PendingCount 返回未确认缓冲中的区块数（内存稳定性断言用）。
func (ix *Indexer) PendingCount() int {
	ix.stateMu.Lock()
	defer ix.stateMu.Unlock()
	return len(ix.pending)
}

// lookup 同时覆盖未确认缓冲与已确认存储（缓冲优先）。
// 调用方必须已持有 stateMu。
func (ix *Indexer) lookup(h uint64) (*Block, bool) {
	if b, ok := ix.pending[h]; ok {
		return b, true
	}
	if b := ix.store.BlockAt(h); b != nil {
		return b, true
	}
	return nil, false
}

// Submit 接收一个区块（允许乱序、重复、属于重组新链）。
// 返回错误可用 errors.Is 区分为 ErrGapAhead / ErrAlreadyCanonical /
// ErrAlreadyBuffered；这些多数是“可区分的非致命原因”，调用方可继续投递。
// 哈希冲突不会作为错误冒泡——重组会被就地处理并收敛（流水可见）。
func (ix *Indexer) Submit(b *Block) error {
	if b == nil {
		return ErrNotFound
	}
	ix.stopMu.Lock()
	stopped := ix.stopped
	ix.stopMu.Unlock()
	if stopped {
		return ErrStopped
	}

	// 所有状态变更串行化；Submit 与后台循环、测试读取互斥。
	ix.stateMu.Lock()
	defer ix.stateMu.Unlock()

	// 区块到达本身意味着链头至少不低于它。
	if !ix.tipSet || b.Height > ix.tip {
		ix.tip, ix.tipSet = b.Height, true
	}

	h := b.Height
	if existing, ok := ix.lookup(h); ok {
		if existing.Hash == b.Hash {
			// 重复投递：按“已落库 / 缓冲中”分别给出可区分原因。
			if _, inStore := ix.store.HashAt(h); inStore {
				ix.logf(LogEntry{Kind: LogReject, Height: h, Hash: b.Hash,
					Node: b.NodeHint, Reason: ReasonDupCanonical,
					Detail: "identical block already confirmed"})
				return ErrAlreadyCanonical
			}
			ix.logf(LogEntry{Kind: LogReject, Height: h, Hash: b.Hash,
				Node: b.NodeHint, Reason: ReasonDupBuffer,
				Detail: "identical block already buffered"})
			return ErrAlreadyBuffered
		}
		// 同高度哈希冲突 -> 重组判定。
		dec := detectReorg(ix.lookup, b)
		if !dec.Detected {
			// 理论上不会发生（哈希不同必然至少在 h 处分叉）。
			return ErrHashConflict
		}
		ix.applyReorg(dec, b)
		// 重组后该块进入新区间缓冲。
		ix.pending[h] = b
		ix.logf(LogEntry{Kind: LogArrive, Height: h, Hash: b.Hash, Node: b.NodeHint,
			Detail: "new-chain block after reorg"})
		ix.advance()
		return nil
	}

	// 全新高度：先检查缓冲容量（内存有界）。
	if len(ix.pending) >= maxBuffered {
		ix.logf(LogEntry{Kind: LogReject, Height: h, Hash: b.Hash,
			Node: b.NodeHint, Reason: ReasonGapAhead,
			Detail: "buffer full / block too far ahead, refuse to grow unbounded"})
		return ErrGapAhead
	}

	// 缺口检查：[next, h) 之间有任何缺失高度 -> 缓冲等待，不静默跳过。
	if h > ix.next {
		for miss := ix.next; miss < h; miss++ {
			if _, have := ix.lookup(miss); !have {
				ix.pending[h] = b
				ix.logf(LogEntry{Kind: LogArrive, Height: h, Hash: b.Hash, Node: b.NodeHint})
				ix.logf(LogEntry{Kind: LogReject, Height: h, Hash: b.Hash,
					Node: b.NodeHint, Reason: ReasonGapAhead,
					Detail: "missing h=" + uitoa(miss) + ", buffered and waiting"})
				return ErrGapAhead
			}
		}
	}

	ix.pending[h] = b
	ix.logf(LogEntry{Kind: LogArrive, Height: h, Hash: b.Hash, Node: b.NodeHint})
	ix.advance()
	return nil
}

// applyReorg 执行一次重组：清除分叉高度以上的旧缓冲、回滚已确认区块，
// 并记录 reorg/detach 流水与后续 attach 区间。
// 调用方必须已持有 stateMu。
func (ix *Indexer) applyReorg(dec reorgDecision, incoming *Block) {
	ix.logf(LogEntry{
		Kind: LogReorg, Height: incoming.Height, Hash: incoming.Hash,
		OldHash: dec.OldHash, NewHash: dec.NewHash, Detail: dec.Reason,
	})
	fork := dec.ForkHeight
	// 1) 只清除属于旧链的未确认缓冲：父指针接在“旧链同高度哈希”上的区块。
	//    父指针未知（零哈希）或已指向新链的区块必须保留，
	//    否则会把乱序先到的新链区块误删、造成虚假缺口。
	oldHashes := ix.store.CanonicalHashes()
	for hh, pb := range ix.pending {
		if hh < fork {
			continue
		}
		if hh == fork {
			delete(ix.pending, hh) // 冲突高度本身一定是旧块（新块就是 incoming）
			continue
		}
		if oldParent, known := oldHashes[pb.Height-1]; known && pb.Parent == oldParent {
			delete(ix.pending, hh)
		}
		// 未知父链的可疑残留不在此删除：Store 的父哈希链式校验会阻止它错接。
	}
	// 2) 已确认部分逐块撤销（Store 只允许从 tip 向下摘）。
	oldTip, hadConfirmed := ix.store.Tip()
	if hadConfirmed {
		applyRollback(ix.store, fork, oldTip, ix.logf)
	}
	// 3) 推进位置与重放区间。
	if tip, ok := ix.store.Tip(); ok {
		ix.next = tip + 1
	} else {
		ix.next = ix.cfg.StartHeight
	}
	if hadConfirmed {
		// 回滚区间 [fork, oldTip] 内的新区块属于“重放到规范链”。
		ix.reorgFrom, ix.reorgTo, ix.hasReorgSpan = fork, oldTip, true
	} else {
		ix.hasReorgSpan = false
	}
}

// advance 把连续且达到确认深度的区块从缓冲串行落库。
// 这是唯一调用 Store.PutConfirmed 的地方，保证顺序与一致性。
// 调用方必须已持有 stateMu。
func (ix *Indexer) advance() {
	for {
		b, ok := ix.pending[ix.next]
		if !ok {
			return // 缺口仍在，等待
		}
		if ix.tipSet && b.Height+ix.cfg.Confirmations > ix.tip {
			ix.logf(LogEntry{Kind: LogBuffer, Height: b.Height, Hash: b.Hash,
				Detail: "below confirmation depth, buffered; tip=" + uitoa(ix.tip)})
			return
		}
		// 父哈希链式校验：若前一高度已确认但父哈希对不上，说明规范链在
		// next 以下发生了重组（冲突块直接落在 next，没有同高度冲突信号）。
		// 回退一格并让取数循环向下重新寻找分叉点，绝不能在此静默丢弃或错接。
		if b.Height > 0 {
			if prevHash, ok := ix.store.HashAt(b.Height - 1); ok && b.Parent != prevHash {
				ix.logf(LogEntry{
					Kind:    LogReorg,
					Height:  b.Height,
					Hash:    b.Hash,
					OldHash: prevHash,
					NewHash: b.Parent,
					Detail: "parent mismatch at tip: block parent " + shortHash(b.Parent) +
						" != canonical " + shortHash(prevHash) +
						", rewinding next to h=" + uitoa(b.Height-1) + " to locate fork",
				})
				ix.next = b.Height - 1
				continue // b 仍留在 pending，下一轮从更低高度补拉
			}
		}
		if !ix.store.PutConfirmed(b) {
			// 极端情况：与存储不一致，记录后停止推进以免错误落库。
			ix.logf(LogEntry{Kind: LogReject, Height: b.Height, Hash: b.Hash,
				Reason: ReasonHashConflict, Detail: "store rejected non-contiguous put"})
			return
		}
		delete(ix.pending, ix.next)
		if ix.hasReorgSpan && b.Height >= ix.reorgFrom && b.Height <= ix.reorgTo {
			ix.logf(LogEntry{Kind: LogAttach, Height: b.Height, Hash: b.Hash,
				Detail: "replay onto new canonical chain"})
			if b.Height == ix.reorgTo {
				ix.hasReorgSpan = false
			}
		} else {
			ix.hasReorgSpan = false
			ix.logf(LogEntry{Kind: LogConfirm, Height: b.Height, Hash: b.Hash,
				Detail: "confirmation depth reached, committed"})
		}
		ix.next++
	}
}

// OfferTip 上报节点链头高度（驱动确认判定）。
func (ix *Indexer) OfferTip(tip uint64) {
	ix.stateMu.Lock()
	defer ix.stateMu.Unlock()
	if !ix.tipSet || tip > ix.tip {
		ix.tip, ix.tipSet = tip, true
	}
	ix.advance()
}

// Run 启动后台拉取循环（重复调用安全：已运行则直接返回）。
func (ix *Indexer) Run(ctx context.Context) error {
	ix.stopMu.Lock()
	if ix.cancel != nil {
		ix.stopMu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	ix.cancel = cancel
	ix.stopMu.Unlock()

	go ix.loop(ctx)
	return nil
}

// loop 周期性：探测链头 -> 有界批量并发拉取 -> 按高度排序串行提交。
func (ix *Indexer) loop(ctx context.Context) {
	defer close(ix.done)
	ticker := time.NewTicker(ix.cfg.RunInterval)
	defer ticker.Stop()
	// 每个轮询周期重置隔离名单：恢复中的节点可重新加入多数派，
	// 持续作恶的节点会在下个周期被再次隔离并记录原因。
	quarantined := make(map[string]bool)

	for {
		ix.pollOnce(ctx, quarantined)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// pollOnce 执行单轮“探活链头 + 批量拉取到 next 连续可用或出错为止”。
func (ix *Indexer) pollOnce(ctx context.Context, quarantined map[string]bool) {
	tip, ok := ix.probeTip(ctx)
	if !ok {
		return // 所有节点不可用：本轮跳过，下轮重试（不丢任何状态）
	}
	ix.OfferTip(tip)

	if ix.next > tip {
		return
	}
	to := tip
	if batch := ix.next + uint64(ix.cfg.MaxInFlight) - 1; batch < to {
		to = batch
	}
	results := make([]fetchResult, 0, to-ix.next+1)
	for r := range concurrentFetch(ctx, ix.clients, ix.next, to, ix.cfg.MaxInFlight, ix.logf) {
		results = append(results, r)
	}
	// 并发取数结果必须按高度排序后串行提交，杜绝乱序落库。
	sort.Slice(results, func(i, j int) bool { return results[i].height < results[j].height })
	for _, r := range results {
		if r.err != nil {
			// 如全部节点不可用：等待下轮重试，不跳过任何高度。
			return
		}
		_ = ix.Submit(r.block) // gap/dup 等原因已在流水中区分，循环继续
	}
}

// probeTip 查询各健康节点链头，取最大值；全部失败时 ok=false。
func (ix *Indexer) probeTip(ctx context.Context) (uint64, bool) {
	type tr struct {
		tip  uint64
		name string
		err  error
	}
	ch := make(chan tr, len(ix.clients))
	var wg sync.WaitGroup
	for _, c := range ix.clients {
		if !c.Healthy() {
			continue
		}
		wg.Add(1)
		go func(c Client) {
			defer wg.Done()
			callCtx, cancel := context.WithTimeout(ctx, defaultPerCallTimeout)
			defer cancel()
			tip, err := c.ChainTip(callCtx)
			ch <- tr{tip: tip, name: c.Name(), err: err}
		}(c)
	}
	wg.Wait()
	close(ch)
	var maxTip uint64
	ok := false
	var fails []string
	for r := range ch {
		if r.err != nil {
			fails = append(fails, r.name+":"+r.err.Error())
			continue
		}
		ix.logf(LogEntry{Kind: LogFetch, Height: r.tip, Node: r.name,
			Detail: "chain tip probe"})
		if !ok || r.tip > maxTip {
			maxTip, ok = r.tip, true
		}
	}
	if !ok {
		ix.logf(LogEntry{Kind: LogReject, Reason: ReasonNoHealthyNode,
			Detail: "tip probe failed: " + joinReasons(fails)})
	}
	return maxTip, ok
}

// Stop 停止后台循环。
func (ix *Indexer) Stop() {
	ix.stopMu.Lock()
	ix.stopped = true
	if ix.cancel != nil {
		ix.cancel()
	}
	ix.stopMu.Unlock()
	if ix.done != nil {
		select {
		case <-ix.done:
		case <-time.After(time.Second):
		}
	}
}

func uitoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
