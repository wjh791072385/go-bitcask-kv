package index

import (
	"go-bitcask-kv/data"
)

// Indexer 抽象索引接口
type Indexer interface {
	// Put 插入索引
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 获取索引
	Get(key []byte) *data.LogRecordPos

	// Delete 删除索引
	Delete(key []byte) bool
}

type IndexerType = int8

const (
	// BtreeIndex 索引
	BtreeIndex IndexerType = iota + 1

	// 后续可扩展
)

func NewIndexer(typ IndexerType) Indexer {
	switch typ {
	case BtreeIndex:
		return NewBtreeIndexer()
	default:
		panic("unsurportted index type")
	}
}
