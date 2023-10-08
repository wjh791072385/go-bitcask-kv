package index

import (
	"go-bitcask-kv/data"
)

type IndexerType = int8

const (
	// BtreeIndex B树索引
	BtreeIndex IndexerType = iota + 1

	// ARTIndex 自适应基数索引
	ARTIndex

	// 后续可扩展
)

// Indexer 抽象索引接口
type Indexer interface {
	// Put 插入索引
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 获取索引
	Get(key []byte) *data.LogRecordPos

	// Delete 删除索引
	Delete(key []byte) bool

	Iterator(reverse bool) IndexerIterator

	Size() int
}

func NewIndexer(typ IndexerType) Indexer {
	switch typ {
	case BtreeIndex:
		return NewBtreeIndexer()
	case ARTIndex:
		return NewART()
	default:
		panic("unSupported index type")
	}
}
