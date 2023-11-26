package index

import (
	"bytes"
	"github.com/google/btree"
	"go-bitcask-kv/data"
	"sort"
	"sync"
)

// Btree 索引，封装google的btree kv
type Btree struct {
	// Write operations are not safe for concurrent mutation by multiple
	// goroutines, but Read operations are.
	tree *btree.BTree

	lock *sync.RWMutex
}

func NewBtreeIndexer() *Btree {
	return &Btree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *Btree) Put(key []byte, pos *data.LogRecordPos) (*data.LogRecordPos, bool) {
	it := &BTreeItem{key: key, pos: pos}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*BTreeItem).pos, true
}

func (bt *Btree) Get(key []byte) *data.LogRecordPos {
	it := &BTreeItem{key: key}
	btItem := bt.tree.Get(it)
	if btItem == nil {
		return nil
	}

	return btItem.(*BTreeItem).pos
}

func (bt *Btree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &BTreeItem{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*BTreeItem).pos, true
}

func (bt *Btree) Iterator(reverse bool) IndexerIterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return NewBtreeIterator(bt.tree, reverse)
}

func (bt *Btree) Size() int {
	return bt.tree.Len()
}

func (bt *Btree) Close() error {
	return nil
}

// BTreeItem 实现Btree的节点
type BTreeItem struct {
	key []byte
	pos *data.LogRecordPos
}

func (item *BTreeItem) Less(bi btree.Item) bool {
	// bi需要为*BTreeItem 指针类型
	// 从小到大进行排序
	return bytes.Compare(item.key, bi.(*BTreeItem).key) < 0
}

// BtreeIterator Btree索引迭代器
type BtreeIterator struct {
	// 当前下标
	curIndex int

	// 是否反向遍历
	reverse bool

	// 存放Item指针数组
	values []*BTreeItem
}

func NewBtreeIterator(tree *btree.BTree, reverse bool) *BtreeIterator {
	var idx = 0
	values := make([]*BTreeItem, tree.Len())

	//这个回调函数参数需要是 Btree中的接口类型
	saveValues := func(it btree.Item) bool {
		// 判断是当前自定义的Item类型
		values[idx] = it.(*BTreeItem)
		idx++

		// 返回false则会终止遍历
		return true
	}

	if reverse {
		// 反向进行
		tree.Descend(saveValues)
	} else {
		// Ascend会针对BTree中的每个结点执行指定函数
		tree.Ascend(saveValues)
	}

	// reverse决定数据反向或者正向存储，因此curIndex为0
	return &BtreeIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}

}

func (bti *BtreeIterator) Rewind() {
	bti.curIndex = 0
}

func (bti *BtreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

func (bti *BtreeIterator) Next() {
	bti.curIndex++
}

func (bti *BtreeIterator) Valid() bool {
	return bti.curIndex < len(bti.values)
}

func (bti *BtreeIterator) Key() []byte {
	return bti.values[bti.curIndex].key
}

func (bti *BtreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.curIndex].pos
}

func (bti *BtreeIterator) Close() {
	// 清空数组
	bti.values = nil
}
