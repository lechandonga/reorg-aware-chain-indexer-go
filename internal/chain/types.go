// Package chain 定义链上数据的核心类型。
package chain

import "fmt"

// Block 表示一个区块。
type Block struct {
	Height     uint64
	Hash       string
	ParentHash string
	// Payload 是区块携带的业务数据（模拟转账等）。
	Payload []string
}

// Key 返回区块的唯一标识。
func (b Block) Key() string { return fmt.Sprintf("%d:%s", b.Height, b.Hash) }

// RejectReason 描述区块被拒绝或需要等待的原因，便于区分。
type RejectReason int

const (
	ReasonNone RejectReason = iota
	// ReasonGap 高度缺口：期望高度未到，先等待。
	ReasonGap
	// ReasonDuplicate 重复投递，已见过该区块。
	ReasonDuplicate
	// ReasonOrphan 父哈希与当前链头不匹配，可能是乱序或重组前兆。
	ReasonOrphan
	// ReasonUnconfirmed 未达到确认深度，暂不允许落库。
	ReasonUnconfirmed
	// ReasonConflict 与已索引的同高度区块哈希冲突，触发重组判定。
	ReasonConflict
)

func (r RejectReason) String() string {
	switch r {
	case ReasonNone:
		return "none"
	case ReasonGap:
		return "gap:等待缺失高度"
	case ReasonDuplicate:
		return "duplicate:重复投递"
	case ReasonOrphan:
		return "orphan:父哈希不匹配"
	case ReasonUnconfirmed:
		return "unconfirmed:未达确认深度"
	case ReasonConflict:
		return "conflict:同高度哈希冲突"
	}
	return "unknown"
}

// RejectError 是可区分的拒绝错误。
type RejectError struct {
	Reason RejectReason
	Height uint64
	Detail string
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("高度 %d 被拒绝: %s (%s)", e.Height, e.Reason, e.Detail)
}
