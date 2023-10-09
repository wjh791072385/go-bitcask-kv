package index

import (
	"github.com/stretchr/testify/assert"
	"go-bitcask-kv/data"
	"os"
	"path/filepath"
	"testing"
)

var indexType = BtreeIndex
var index = NewIndexer(indexType, "")

func TestIndex(t *testing.T) {
	// 测试BTree
	indexType = BtreeIndex
	index = NewIndexer(indexType, "")
	TestIndex_Put(t)
	TestIndex_Get(t)
	TestIndex_Delete(t)
	TestIndex_Iterator(t)

	// 测试ART
	indexType = ARTIndex
	index = NewIndexer(indexType, "")
	TestIndex_Put(t)
	TestIndex_Get(t)
	TestIndex_Delete(t)
	TestIndex_Iterator(t)

	// 测试BPlusTree
	indexType = BPlusTreeIndex
	indexPath := initBPlusTree()
	index = NewIndexer(indexType, indexPath)
	TestIndex_Put(t)
	TestIndex_Get(t)
	TestIndex_Delete(t)
	TestIndex_Iterator(t)
	clearBPlusTree()
}

func initBPlusTree() string {
	path := filepath.Join(os.TempDir(), "BPlusTree")
	_ = os.MkdirAll(path, os.ModePerm)
	return path
}

func clearBPlusTree() {
	bp := index.(*BPlusTree)
	bp.tree.Close()
	if err := os.RemoveAll(filepath.Join(os.TempDir(), "BPlusTree")); err != nil {
		panic("clear BPlusTree failed")
	}
}

func TestIndex_Put(t *testing.T) {
	//res1 := index.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	//assert.True(t, res1)

	res2 := index.Put([]byte("abc"), &data.LogRecordPos{Fid: 100, Offset: 10010})
	assert.True(t, res2)
}

func TestIndex_Get(t *testing.T) {
	// 不需要测试写入key为nil，因为DB层面的put会禁止nil的key
	//res1 := index.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	//assert.True(t, res1)
	//
	//data1 := index.Get(nil)
	//assert.Equal(t, uint32(1), data1.Fid)
	//assert.Equal(t, int64(100), data1.Offset)

	// 测试写入 读取
	res2 := index.Put([]byte("abc"), &data.LogRecordPos{Fid: 100, Offset: 10010})
	assert.True(t, res2)

	data2 := index.Get([]byte("abc"))
	assert.Equal(t, uint32(100), data2.Fid)
	assert.Equal(t, int64(10010), data2.Offset)

	// 测试覆盖
	res2 = index.Put([]byte("abc"), &data.LogRecordPos{Fid: 888, Offset: 7777})
	assert.True(t, res2)
	data2 = index.Get([]byte("abc"))
	assert.Equal(t, uint32(888), data2.Fid)
	assert.Equal(t, int64(7777), data2.Offset)
}

func TestIndex_Delete(t *testing.T) {
	// 删除不存在的元素
	res := index.Delete([]byte("io"))
	assert.False(t, res)

	// 删除正常元素
	res1 := index.Put([]byte("abc"), &data.LogRecordPos{Fid: 100, Offset: 10010})
	assert.True(t, res1)

	data1 := index.Get([]byte("abc"))
	assert.Equal(t, uint32(100), data1.Fid)
	assert.Equal(t, int64(10010), data1.Offset)

	res1 = index.Delete([]byte("abc"))
	assert.True(t, res1)
	data1 = index.Get([]byte("abc"))
	assert.Nil(t, data1)
}

func TestIndex_Iterator(t *testing.T) {
	// 获取空迭代器
	it1 := index.Iterator(false)
	assert.NotNil(t, it1)
	assert.False(t, it1.Valid())
	it1.Close()

	// 插入1个元素之后使用迭代器进行遍历
	res1 := index.Put([]byte("abc"), &data.LogRecordPos{Fid: 100, Offset: 10010})
	assert.True(t, res1)
	it2 := index.Iterator(false)
	assert.NotNil(t, it2)
	assert.True(t, it2.Valid())
	assert.Equal(t, []byte("abc"), it2.Key())
	assert.Equal(t, uint32(100), it2.Value().Fid)
	it2.Next()
	assert.False(t, it2.Valid())
	it2.Close()

	// 插入多个元素进行遍历
	res2 := index.Put([]byte("abcAbc"), &data.LogRecordPos{Fid: 101, Offset: 10010})
	assert.True(t, res2)
	res3 := index.Put([]byte("abcAbcAbc"), &data.LogRecordPos{Fid: 102, Offset: 10010})
	assert.True(t, res3)
	res4 := index.Put([]byte("abcAbcAbcAbc"), &data.LogRecordPos{Fid: 103, Offset: 10010})
	assert.True(t, res4)

	it3 := index.Iterator(true)
	for i := 0; it3.Valid(); it3.Next() {
		i++
		assert.Equal(t, uint32(104-i), it3.Value().Fid)
	}
	it3.Close()

	// 测试正向seek, 找出>="abcAbcAbc"的元素
	it4 := index.Iterator(false)
	it4.Seek([]byte("abcAbcAbc"))
	cnt := 0
	for ; it4.Valid(); it4.Next() {
		cnt++
	}
	assert.Equal(t, 2, cnt)
	it4.Close()

	// 测试反向seek
	it5 := index.Iterator(true)
	it5.Seek([]byte("abcAbcAbc"))
	cnt = 0
	for ; it5.Valid(); it5.Next() {
		cnt++
	}
	assert.Equal(t, 3, cnt)
	it5.Close()
}
