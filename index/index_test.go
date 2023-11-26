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
	// 不允许key为nil，art中key为nil后，删除时无法删除
	//是否允许key为nil, 测试完后删除nil
	//res1, updated := index.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	//assert.Nil(t, res1)
	//assert.False(t, updated)
	//_, deleted := index.Delete(nil)
	//assert.True(t, deleted)
	//t.Log(deleted)

	res2, updated := index.Put([]byte("put1"), &data.LogRecordPos{Fid: 100, Offset: 10010})
	assert.Nil(t, res2)
	assert.False(t, updated)

	res3, updated := index.Put([]byte("put2"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res3)
	assert.False(t, updated)

	// 重复的key会获取到之前的记录
	res4, updated := index.Put([]byte("put2"), &data.LogRecordPos{Fid: 3, Offset: 4})
	assert.NotNil(t, res4)
	assert.True(t, updated)
	assert.Equal(t, uint32(1), res4.Fid)
	assert.Equal(t, int64(2), res4.Offset)

	// 清理掉自己的元素
	_, deleted := index.Delete([]byte("put1"))
	assert.True(t, deleted)
	_, deleted = index.Delete([]byte("put2"))
	assert.True(t, deleted)

}

func TestIndex_Get(t *testing.T) {
	//// 是否允许key为nil
	// 不允许
	//res1, updated := index.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	//assert.Nil(t, res1)
	//assert.False(t, updated)
	//
	//data1 := index.Get(nil)
	//assert.Equal(t, uint32(1), data1.Fid)
	//assert.Equal(t, int64(100), data1.Offset)
	//_, deleted := index.Delete(nil)
	//assert.True(t, deleted)

	// 测试写入 读取
	res2, updated := index.Put([]byte("get1"), &data.LogRecordPos{Fid: 100, Offset: 10010})
	assert.Nil(t, res2)
	assert.False(t, updated)

	data2 := index.Get([]byte("get1"))
	assert.Equal(t, uint32(100), data2.Fid)
	assert.Equal(t, int64(10010), data2.Offset)

	// 测试覆盖, 会得到之前的数据
	res2, updated = index.Put([]byte("get1"), &data.LogRecordPos{Fid: 888, Offset: 7777})
	assert.NotNil(t, res2)
	assert.True(t, updated)
	assert.Equal(t, uint32(100), res2.Fid)
	assert.Equal(t, int64(10010), res2.Offset)

	data2 = index.Get([]byte("get1"))
	assert.Equal(t, uint32(888), data2.Fid)
	assert.Equal(t, int64(7777), data2.Offset)

	_, deleted := index.Delete([]byte("get1"))
	assert.True(t, deleted)
}

func TestIndex_Delete(t *testing.T) {
	// 删除不存在的元素
	res, deleted := index.Delete([]byte("no-exist"))
	assert.Nil(t, res)
	assert.False(t, deleted)

	// 删除正常元素
	res1, _ := index.Put([]byte("delete1"), &data.LogRecordPos{Fid: 100, Offset: 10010})
	assert.Nil(t, res1)

	data1 := index.Get([]byte("delete1"))
	assert.Equal(t, uint32(100), data1.Fid)
	assert.Equal(t, int64(10010), data1.Offset)

	res1, deleted = index.Delete([]byte("delete1"))
	assert.NotNil(t, res1)
	assert.True(t, deleted)
	assert.Equal(t, uint32(100), res1.Fid)
	assert.Equal(t, int64(10010), res1.Offset)

	data1 = index.Get([]byte("delete1"))
	assert.Nil(t, data1)
}

func TestIndex_Iterator(t *testing.T) {
	// 获取空迭代器
	it1 := index.Iterator(false)
	assert.NotNil(t, it1)
	assert.False(t, it1.Valid())
	it1.Close()

	// 插入1个元素之后使用迭代器进行遍历
	res1, _ := index.Put([]byte("abc"), &data.LogRecordPos{Fid: 100, Offset: 10010})
	assert.Nil(t, res1)
	it2 := index.Iterator(false)
	assert.NotNil(t, it2)
	assert.True(t, it2.Valid())
	assert.Equal(t, []byte("abc"), it2.Key())
	assert.Equal(t, uint32(100), it2.Value().Fid)
	it2.Next()
	assert.False(t, it2.Valid())
	it2.Close()

	// 插入多个元素进行遍历
	res2, _ := index.Put([]byte("abcAbc"), &data.LogRecordPos{Fid: 101, Offset: 10010})
	assert.Nil(t, res2)
	res3, _ := index.Put([]byte("abcAbcAbc"), &data.LogRecordPos{Fid: 102, Offset: 10010})
	assert.Nil(t, res3)
	res4, _ := index.Put([]byte("abcAbcAbcAbc"), &data.LogRecordPos{Fid: 103, Offset: 10010})
	assert.Nil(t, res4)

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
