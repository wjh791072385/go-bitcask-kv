package bitcaskKV

import (
	"bytes"

	"go-bitcask-kv/index"
)

// Iterator 面向DB层面的迭代器
type Iterator struct {
	db        *DB
	indexIter index.IndexerIterator
	option    IteratorOption
}

// NewIterator 初始化迭代器
func (db *DB) NewIterator(opt IteratorOption) *Iterator {
	indexIter := db.index.Iterator(opt.reverse)
	return &Iterator{
		db:        db,
		indexIter: indexIter,
		option:    opt,
	}
}

func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
}

func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value 注意这里是返回value的具体值，而索引迭代器是返回pos信息
func (it *Iterator) Value() ([]byte, error) {
	pos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(pos)
}

func (it *Iterator) Close() {
	it.indexIter.Close()
}

func (it *Iterator) skipToNext() {
	prefixLen := len(it.option.prefix)
	if prefixLen == 0 {
		// prefix默认为空，表示不进行前缀匹配
		return
	}

	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(it.option.prefix, key[:prefixLen]) == 0 {
			// 找到第一个匹配的元素，结束循环
			break
		}
	}
}
