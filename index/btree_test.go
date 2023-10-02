package index

import (
	"github.com/stretchr/testify/assert"
	"go-bitcask-kv/data"
	"testing"
)

func TestBtree_Put(t *testing.T) {
	bt := NewBtreeIndexer()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Put([]byte("abc"), &data.LogRecordPos{Fid: 100, Offset: 10010})
	assert.True(t, res2)
}

func TestBtree_Get(t *testing.T) {
	bt := NewBtreeIndexer()

	// 测试写入key为nil
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	data1 := bt.Get(nil)
	assert.Equal(t, uint32(1), data1.Fid)
	assert.Equal(t, int64(100), data1.Offset)

	// 测试写入 读取
	res2 := bt.Put([]byte("abc"), &data.LogRecordPos{Fid: 100, Offset: 10010})
	assert.True(t, res2)

	data2 := bt.Get([]byte("abc"))
	assert.Equal(t, uint32(100), data2.Fid)
	assert.Equal(t, int64(10010), data2.Offset)

	// 测试覆盖
	res2 = bt.Put([]byte("abc"), &data.LogRecordPos{Fid: 888, Offset: 7777})
	assert.True(t, res2)
	data2 = bt.Get([]byte("abc"))
	assert.Equal(t, uint32(888), data2.Fid)
	assert.Equal(t, int64(7777), data2.Offset)
}

func TestBtree_Delete(t *testing.T) {
	bt := NewBtreeIndexer()

	// 删除不存在的元素
	res := bt.Delete([]byte("io"))
	assert.False(t, res)

	// 删除正常元素
	res1 := bt.Put([]byte("abc"), &data.LogRecordPos{Fid: 100, Offset: 10010})
	assert.True(t, res1)

	data1 := bt.Get([]byte("abc"))
	assert.Equal(t, uint32(100), data1.Fid)
	assert.Equal(t, int64(10010), data1.Offset)

	res1 = bt.Delete([]byte("abc"))
	assert.True(t, res1)
	data1 = bt.Get([]byte("abc"))
	assert.Nil(t, data1)
}

func TestBtree_Iterator(t *testing.T) {
	bt := NewBtreeIndexer()

	// 获取空迭代器
	it1 := bt.Iterator(false)
	assert.NotNil(t, it1)
	assert.False(t, it1.Valid())
	it1.Close()

	// 插入1个元素之后使用迭代器进行遍历
	res1 := bt.Put([]byte("abc"), &data.LogRecordPos{Fid: 100, Offset: 10010})
	assert.True(t, res1)
	it2 := bt.Iterator(false)
	assert.NotNil(t, it2)
	assert.True(t, it2.Valid())
	assert.Equal(t, []byte("abc"), it2.Key())
	assert.Equal(t, uint32(100), it2.Value().Fid)
	it2.Next()
	assert.False(t, it2.Valid())
	it2.Close()

	// 插入多个元素进行遍历
	res2 := bt.Put([]byte("abcAbc"), &data.LogRecordPos{Fid: 101, Offset: 10010})
	assert.True(t, res2)
	res3 := bt.Put([]byte("abcAbcAbc"), &data.LogRecordPos{Fid: 102, Offset: 10010})
	assert.True(t, res3)
	res4 := bt.Put([]byte("abcAbcAbcAbc"), &data.LogRecordPos{Fid: 103, Offset: 10010})
	assert.True(t, res4)

	it3 := bt.Iterator(true)
	for i := 0; it3.Valid(); it3.Next() {
		i++
		assert.Equal(t, uint32(104-i), it3.Value().Fid)
	}
	it3.Close()

	// 测试正向seek, 找出>="abcAbcAbc"的元素
	it4 := bt.Iterator(false)
	it4.Seek([]byte("abcAbcAbc"))
	cnt := 0
	for ; it4.Valid(); it4.Next() {
		cnt++
	}
	assert.Equal(t, 2, cnt)
	it4.Close()

	// 测试反向seek
	it5 := bt.Iterator(true)
	it5.Seek([]byte("abcAbcAbc"))
	cnt = 0
	for ; it5.Valid(); it5.Next() {
		cnt++
	}
	assert.Equal(t, 3, cnt)
	it5.Close()
}
