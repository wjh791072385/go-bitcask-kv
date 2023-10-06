package index

import "go-bitcask-kv/data"

type IndexerIterator interface {
	// Rewind 重新回到迭代器起点
	Rewind()

	// Seek 根据传入的key, 需要第一个大于（小于）等于key的迭代器
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