package indexer

import (
	"fmt"
	"sync"
)

// LogCollector 是线程安全的高度流水收集器。
// 测试通过它打印“高度流水”与“回滚判定依据”，并对原因做区分断言。
type LogCollector struct {
	mu   sync.Mutex
	logs []LogEntry
}

// NewLogCollector 创建收集器。
func NewLogCollector() *LogCollector { return &LogCollector{} }

// Add 追加一条流水。
func (c *LogCollector) Add(e LogEntry) {
	c.mu.Lock()
	c.logs = append(c.logs, e)
	c.mu.Unlock()
}

// Logger 返回可直接传给 New 的回调函数。
func (c *LogCollector) Logger() func(LogEntry) { return c.Add }

// All 返回流水的副本。
func (c *LogCollector) All() []LogEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]LogEntry, len(c.logs))
	copy(out, c.logs)
	return out
}

// shortHash 将 32 字节哈希压缩为前 6 个十六进制字符，便于阅读。
func shortHash(h Hash) string {
	return fmt.Sprintf("%x", h[:3])
}

// Print 把流水按顺序输出到测试日志，例如：
//
//	[arrive ] h=3 hash=ab12cd node=n1
//	[reject ] h=3 reason=gap_ahead detail=next=1
//	[reorg  ] h=2 fork=1 old=... new=... detail=hash mismatch ...
func (c *LogCollector) Print(p func(string, ...any)) {
	for _, e := range c.All() {
		line := fmt.Sprintf("[%-8s] h=%d hash=%s", e.Kind, e.Height, shortHash(e.Hash))
		if e.Kind == LogReorg {
			line += fmt.Sprintf(" old=%s new=%s", shortHash(e.OldHash), shortHash(e.NewHash))
		}
		if e.Node != "" {
			line += " node=" + e.Node
		}
		if e.Reason != ReasonNone {
			line += " reason=" + string(e.Reason)
		}
		if e.Detail != "" {
			line += " detail=" + e.Detail
		}
		p("%s", line)
	}
}

// Reasons 返回流水中出现过的全部拒绝原因（顺序保留）。
func (c *LogCollector) Reasons() []RejectReason {
	var out []RejectReason
	for _, e := range c.All() {
		if e.Kind == LogReject && e.Reason != ReasonNone {
			out = append(out, e.Reason)
		}
	}
	return out
}

// Has 判断是否出现过指定流水。
func (c *LogCollector) Has(kind LogKind, height uint64) bool {
	for _, e := range c.All() {
		if e.Kind == kind && e.Height == height {
			return true
		}
	}
	return false
}
