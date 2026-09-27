package indexer

// Hash 是区块哈希的固定长度表示。
type Hash [32]byte

// Block 表示链上的一个区块。
type Block struct {
	Height   uint64
	Hash     Hash
	Parent   Hash
	Events   []Event
	NodeHint string // 由哪个节点返回，仅用于诊断
}

// Event 表示区块内可被索引的派生数据（事件/交易）。
type Event struct {
	Key   string
	Value string
}

// LogKind 标记索引流水日志的类别。
type LogKind string

const (
	LogArrive   LogKind = "arrive"   // 区块到达（可能乱序/重复）
	LogBuffer   LogKind = "buffer"   // 进入/留在缓冲
	LogReject   LogKind = "reject"   // 拒绝（含可区分原因）
	LogConfirm  LogKind = "confirm"  // 达到确认深度，正式落库
	LogReorg    LogKind = "reorg"    // 判定重组
	LogDetach   LogKind = "detach"   // 回滚区块
	LogAttach   LogKind = "attach"   // 重放新区块
	LogFailover LogKind = "failover" // 节点故障切换
	LogFetch    LogKind = "fetch"    // 拉取动作
)

// LogEntry 是一条高度流水日志，便于测试打印与断言。
type LogEntry struct {
	Kind    LogKind
	Height  uint64
	Hash    Hash
	OldHash Hash // 重组时旧链哈希
	NewHash Hash // 重组时新链哈希
	Detail  string
	Node    string
	Reason  RejectReason
}

// RejectReason 是拒绝/等待原因的可区分枚举。
type RejectReason string

const (
	ReasonNone           RejectReason = ""
	ReasonGapAhead       RejectReason = "gap_ahead"       // 前方有高度缺口，等待
	ReasonDupCanonical   RejectReason = "dup_canonical"   // 已在规范链上的重复区块
	ReasonDupBuffer      RejectReason = "dup_buffer"      // 已在缓冲中的重复区块
	ReasonHashConflict   RejectReason = "hash_conflict"   // 同高度哈希冲突（疑似重组）
	ReasonNodeQuarantine RejectReason = "node_quarantine" // 节点数据与多数派不一致
	ReasonNoHealthyNode  RejectReason = "no_healthy_node" // 无可用节点
)
