package indexer

import (
	"context"
	"sync"
	"time"
)

// fetcher.go: 多节点故障切换 + 并发取数。
//
// 设计要点：
//   - 对同一高度并发请求所有健康节点，按“多数派哈希”仲裁，
//     少数派节点视为返回矛盾数据 -> 隔离（本次不再使用）并记 failover 流水；
//   - 超时/不可用节点自动跳过；全部不可用 -> ErrNoHealthyNode；
//   - 取数阶段不触碰任何索引状态，绝不会乱序落库；落库由 Indexer 串行完成；
//   - concurrentFetch 用固定大小 worker 池限制在途请求，保证内存有界。

// defaultPerCallTimeout 是单次节点 RPC 的超时，避免慢节点长期占住 worker。
const defaultPerCallTimeout = 200 * time.Millisecond

// fetchResult 是一次区间拉取中单个高度的结果。
type fetchResult struct {
	height uint64
	block  *Block
	client string
	err    error
}

type vote struct {
	block *Block
	from  string
}

// FetchBlockWithFailover 是 fetchBlockWithFailover 的导出包装，供外部/测试直接调用。
// quarantined 为本次调用内被隔离的节点名集合（传空 map 即可）。
func FetchBlockWithFailover(ctx context.Context, clients []Client, height uint64,
	quarantined map[string]bool, log func(LogEntry)) (*Block, error) {
	if quarantined == nil {
		quarantined = map[string]bool{}
	}
	return fetchBlockWithFailover(ctx, clients, height, quarantined, log)
}

// fetchBlockWithFailover 对同一高度做多节点并发取数 + 多数派仲裁。
// quarantined 为本次调用内被隔离的节点名集合（调用方可持久化）。
// 返回的区块来自多数派；日志中记录每次不可用切换与矛盾隔离的原因。
func fetchBlockWithFailover(
	ctx context.Context,
	clients []Client,
	height uint64,
	quarantined map[string]bool,
	log func(LogEntry),
) (*Block, error) {
	type resp struct {
		vote vote
		err  error
	}
	healthy := make([]Client, 0, len(clients))
	for _, c := range clients {
		if c.Healthy() && !quarantined[c.Name()] {
			healthy = append(healthy, c)
		}
	}
	if len(healthy) == 0 {
		return nil, ErrNoHealthyNode
	}

	respCh := make(chan resp, len(healthy))
	var wg sync.WaitGroup
	for _, c := range healthy {
		wg.Add(1)
		go func(c Client) {
			defer wg.Done()
			callCtx, cancel := context.WithTimeout(ctx, defaultPerCallTimeout)
			defer cancel()
			blk, err := c.BlockAt(callCtx, height)
			if err != nil {
				respCh <- resp{err: err}
				return
			}
			respCh <- resp{vote: vote{block: blk, from: c.Name()}}
		}(c)
	}
	wg.Wait()
	close(respCh)

	// 按哈希计票；不可用/超时仅记录切换原因，不参与投票。
	ballot := make(map[Hash][]vote)
	var failures []string
	for r := range respCh {
		if r.err != nil {
			failures = append(failures, r.err.Error())
			continue
		}
		v := r.vote
		ballot[v.block.Hash] = append(ballot[v.block.Hash], v)
	}
	if len(ballot) == 0 {
		log(LogEntry{
			Kind: LogFailover, Height: height,
			Detail: "all healthy nodes failed: " + joinReasons(failures),
		})
		return nil, ErrNoHealthyNode
	}

	// 选出得票最多的哈希；平票时取区块哈希字典序最小者，保证结论确定。
	var winner Hash
	var topVotes []vote
	topN := 0
	for h, vs := range ballot {
		switch {
		case len(vs) > topN:
			topN, winner, topVotes = len(vs), h, vs
		case len(vs) == topN && compareHash(h, winner) < 0:
			winner, topVotes = h, vs
		}
	}

	// 隔离少数派：其哈希与多数派不同的节点。
	quarantineList := logQuarantine(ballot, winner, height, log)
	for _, name := range quarantineList {
		quarantined[name] = true
	}
	if len(failures) > 0 {
		log(LogEntry{
			Kind: LogFailover, Height: height,
			Node:   topVotes[0].from,
			Detail: "bypassed unavailable nodes: " + joinReasons(failures),
		})
	}
	blk := topVotes[0].block
	log(LogEntry{
		Kind:   LogFetch,
		Height: height,
		Hash:   blk.Hash,
		Node:   topVotes[0].from,
		Detail: "majority votes " + itoa(len(topVotes)) + "/" + itoa(len(healthy)),
	})
	return blk, nil
}

// logQuarantine 记录被隔离的少数派节点，返回节点名列表。
func logQuarantine(ballot map[Hash][]vote, winner Hash, height uint64, log func(LogEntry)) []string {
	var out []string
	for h, vs := range ballot {
		if h == winner {
			continue
		}
		for _, v := range vs {
			out = append(out, v.from)
			log(LogEntry{
				Kind:   LogReject,
				Height: height,
				Hash:   h,
				Node:   v.from,
				Reason: ReasonNodeQuarantine,
				Detail: "minority hash " + shortHash(h) + " vs majority " + shortHash(winner),
			})
			log(LogEntry{
				Kind:   LogFailover,
				Height: height,
				Hash:   h,
				Node:   v.from,
				Detail: "node quarantined, switching to majority view",
			})
		}
	}
	return out
}

// concurrentFetch 用固定 worker 池并发拉取 [from,to]（含两端）。
// 结果无序地从返回 channel 输出；调用方必须按 height 排序后串行提交。
// 在途任务数 = min(maxInFlight, 区间长度)，长时间运行时内存占用有界。
func concurrentFetch(
	ctx context.Context,
	clients []Client,
	from, to uint64,
	maxInFlight int,
	log func(LogEntry),
) <-chan fetchResult {
	if maxInFlight <= 0 {
		maxInFlight = 1
	}
	out := make(chan fetchResult, maxInFlight)
	go func() {
		defer close(out)
		jobs := make(chan uint64, maxInFlight)
		var wg sync.WaitGroup
		// 每个 worker 各自维护“本次区间内”的隔离名单，避免坏节点反复拖慢。
		for w := 0; w < maxInFlight; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				q := make(map[string]bool)
				for h := range jobs {
					blk, err := fetchBlockWithFailover(ctx, clients, h, q, log)
					select {
					case out <- fetchResult{height: h, block: blk, err: err}:
					case <-ctx.Done():
						return
					}
					if err != nil {
						return
					}
				}
			}()
		}
		for h := from; h <= to; h++ {
			select {
			case jobs <- h:
			case <-ctx.Done():
				close(jobs)
				wg.Wait()
				return
			}
			if h == to { // to 是 uint64，防止 h++ 溢出
				break
			}
		}
		close(jobs)
		wg.Wait()
	}()
	return out
}

// compareHash 比较两个哈希（字典序），用于平票时的确定性选择。
func compareHash(a, b Hash) int {
	for i := 0; i < len(a); i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

func joinReasons(xs []string) string {
	out := ""
	for i, s := range xs {
		if i > 0 {
			out += "; "
		}
		out += s
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
