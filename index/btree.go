package index

import (
	"bytes"
	"github.com/google/btree"
	"go-bitcask-kv/data"
	"sync"
)


// Btree 索引，封装google的btree kv
type Btree struct {
	// Write operations are not safe for concurrent mutation by multiple
	// goroutines, but Read operations are.
	tree *btree.BTree

	lock *sync.RWMutex
}

func NewIndexer() *Btree {
	return &Btree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *Btree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

func (bt *Btree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btItem := bt.tree.Get(it)
	if btItem == nil {
		return nil
	}

	return btItem.(*Item).pos
}

func (bt *Btree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}

// Item 实现Btree的节点
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (item *Item) Less(bi btree.Item) bool {
	// bi需要为*Item 指针类型
	// 从小到大进行排序
	return bytes.Compare(item.key, bi.(*Item).key) < 0
}


