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

	// BPlusTreeIndex B+树索引
	BPlusTreeIndex

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

	// Close 只是对于B+树才需要， 因为这里B+树借用了一个DB的实现
	Close() error
}

func NewIndexer(typ IndexerType, dirpath string) Indexer {
	switch typ {
	case BtreeIndex:
		return NewBtreeIndexer()
	case ARTIndex:
		return NewART()
	case BPlusTreeIndex:
		return NewBPlusTree(dirpath)
	default:
		panic("unSupported index type")
	}
}

type IndexerIterator interface {
	// Rewind 重新回到迭代器起点
	Rewind()

	// Seek 根据传入的key, 需要第一个大于（小于）等于key的迭代器
	// B树和ART实现Seek都需要将位置信息全量拷贝出来做法性能上不可取
	// B树可以不通过迭代器，而是对外提供Ascend+回调函数的方式对外提供范围查询
	// 如果不需要Seek，完全可以通过B树或者ART自带的迭代器来支持
	// 要支持范围查询，似乎需要Seek，使用B+树或者是更好的选择？
	Seek(key []byte)

	// Next 跳转到下一个key
	Next()

	// Valid 判断当前迭代器是否有效
	Valid() bool

	// Key 返回key
	Key() []byte

	// Value 返回对应的数据信息
	Value() *data.LogRecordPos

	// Close 关闭迭代器，释放对应资源
	Close()
}
