package index

import (
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"go-bitcask-kv/data"
	"sort"
	"sync"
)

type AdaptiveRadixTree struct {
	// 这里不用取地址 因为Tree是一个接口
	tree goart.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.lock.Lock()
	defer art.lock.Unlock()
	art.tree.Insert(key, pos)
	return true
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) bool {
	art.lock.Lock()
	defer art.lock.Unlock()
	_, deleted := art.tree.Delete(key)
	return deleted
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) IndexerIterator {
	return NewARTIterator(art.tree, reverse)
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return art.Size()
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// ARTIterator ART索引迭代器
type ARTIterator struct {
	// 当前下标
	curIndex int

	// 是否反向遍历
	reverse bool

	// 存放Item指针数组
	values []*Item
}

// NewARTIterator 返回ART索引
func NewARTIterator(tree goart.Tree, reverse bool) *ARTIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}

	values := make([]*Item, tree.Size())

	saveFunc := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}

		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	// 通过回调函数存储所有的kv
	tree.ForEach(saveFunc)

	return &ARTIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

func (art *ARTIterator) Rewind() {
	art.curIndex = 0
}

func (art *ARTIterator) Seek(key []byte) {
	if art.reverse {
		art.curIndex = sort.Search(len(art.values), func(i int) bool {
			return bytes.Compare(art.values[i].key, key) <= 0
		})
	} else {
		art.curIndex = sort.Search(len(art.values), func(i int) bool {
			return bytes.Compare(art.values[i].key, key) >= 0
		})
	}
}

func (art *ARTIterator) Next() {
	art.curIndex++
}

func (art *ARTIterator) Valid() bool {
	return art.curIndex < len(art.values)
}

func (art *ARTIterator) Key() []byte {
	return art.values[art.curIndex].key
}

func (art *ARTIterator) Value() *data.LogRecordPos {
	return art.values[art.curIndex].pos
}

func (art *ARTIterator) Close() {
	art.values = nil
}
