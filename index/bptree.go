package index

import (
	"go-bitcask-kv/data"
	"go.etcd.io/bbolt"
	"path/filepath"
)

const (
	BPlusTreeIndexFileName = "BPlusTree-index"
)

var indexBucketName = []byte("bitcask-index")

type BPlusTree struct {
	// 通过B+树实现的存储，内部维护了锁
	tree *bbolt.DB
}

func NewBPlusTree(dirPath string) *BPlusTree {
	bPTree, err := bbolt.Open(filepath.Join(dirPath, BPlusTreeIndexFileName), 0644, nil)
	if err != nil {
		panic("failed to open BPlusTree")
	}

	// 创建对应的bucket, 该db通过bucket来操作数据
	if err := bPTree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in BPlusTree")
	}

	return &BPlusTree{
		tree: bPTree,
	}
}

func (bp *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if err := bp.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		err := bucket.Put(key, data.EncodeLogRecordPos(pos))
		return err
	}); err != nil {
		panic("failed to put kv")
	}
	return true
}

func (bp *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos = nil
	// view中只能读数据
	if err := bp.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		val := bucket.Get(key)

		if len(val) != 0 {
			pos = data.DecodeLogRecordPos(val)
		}

		return nil
	}); err != nil {
		panic("failed to put kv")
	}

	return pos
}

func (bp *BPlusTree) Delete(key []byte) bool {
	var ok = false
	if err := bp.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)

		// 因为要判断是否删除， 要先判断元素是否存在
		val := bucket.Get(key)
		if len(val) != 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete key")
	}
	return ok
}

func (bp *BPlusTree) Size() int {
	// 返回bucket中key的数量
	var size int
	if err := bp.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size")
	}

	return size
}

func (bp *BPlusTree) Close() error {
	return bp.tree.Close()
}

func (bp *BPlusTree) Iterator(reverse bool) IndexerIterator {
	return newBPlusTreeIterator(bp.tree, reverse)
}

// BPlusTreeIterator B+树迭代器
// 直接使用提供的游标cursor
type BPlusTreeIterator struct {
	tx        *bbolt.Tx
	cursor    *bbolt.Cursor
	reverse   bool
	currKey   []byte
	currValue []byte
}

func newBPlusTreeIterator(tree *bbolt.DB, reverse bool) *BPlusTreeIterator {
	// 开启一个事务  保证迭代器有效
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}

	bpi := &BPlusTreeIterator{
		tx:        tx,
		cursor:    tx.Bucket(indexBucketName).Cursor(),
		reverse:   reverse,
		currKey:   nil,
		currValue: nil,
	}

	// 避免初始化后位空
	bpi.Rewind()
	return bpi
}

func (bpi *BPlusTreeIterator) Rewind() {
	if bpi.reverse {
		bpi.currKey, bpi.currValue = bpi.cursor.Last()
	} else {
		bpi.currKey, bpi.currValue = bpi.cursor.First()
	}
}

func (bpi *BPlusTreeIterator) Seek(key []byte) {
	bpi.currKey, bpi.currValue = bpi.cursor.Seek(key)
}

func (bpi *BPlusTreeIterator) Next() {
	if bpi.reverse {
		bpi.currKey, bpi.currValue = bpi.cursor.Prev()
	} else {
		bpi.currKey, bpi.currValue = bpi.cursor.Next()
	}
}

func (bpi *BPlusTreeIterator) Valid() bool {
	// 当前key不为空则表示有效
	return len(bpi.currKey) != 0
}

func (bpi *BPlusTreeIterator) Key() []byte {
	return bpi.currKey
}

func (bpi *BPlusTreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpi.currValue)
}

func (bpi *BPlusTreeIterator) Close() {
	// Rollback closes the transaction and ignores all previous updates. Read-only
	// transactions must be rolled back and not committed.
	_ = bpi.tx.Rollback()
}
