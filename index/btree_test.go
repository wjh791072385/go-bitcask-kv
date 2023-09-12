package index

import (
	"github.com/stretchr/testify/assert"
	"go-bitcask-kv/data"
	"testing"
)

func TestBtree_Put(t *testing.T) {
	bt := NewIndexer()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Put([]byte("abc"), &data.LogRecordPos{Fid: 100, Offset: 10010})
	assert.True(t, res2)
}

func TestBtree_Get(t *testing.T) {
	bt := NewIndexer()

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
	bt := NewIndexer()

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