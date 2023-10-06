package bitcaskKV

import (
	"github.com/stretchr/testify/assert"
	"go-bitcask-kv/utils"
	"os"
	"testing"
)

func TestWriteBatch_Put(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-writeBatch-put")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 初始化WriteBatch
	wb := db.NewWriteBatch(DefaultWriteBachOption)
	val1 := utils.GetTestRandomValue(10)
	err = wb.Put(utils.GetTestKey(1), val1)
	assert.Nil(t, err)

	val2 := utils.GetTestRandomValue(10)
	err = wb.Put(utils.GetTestKey(2), val2)
	assert.Nil(t, err)

	// 此时还未提交，两次Put都应该没有生效
	val, err := db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)

	val, err = db.Get(utils.GetTestKey(2))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)

	// 提交
	err = wb.Commit()
	assert.Nil(t, err)

	// 再次检查数据
	val, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, val1, val)

	val, err = db.Get(utils.GetTestKey(2))
	assert.Equal(t, val2, val)
}

func TestWriteBatch_Delete(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-writeBatch-put")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 初始化WriteBatch
	wb := db.NewWriteBatch(DefaultWriteBachOption)
	val1 := utils.GetTestRandomValue(10)
	err = wb.Put(utils.GetTestKey(1), val1)
	assert.Nil(t, err)

	val2 := utils.GetTestRandomValue(10)
	err = wb.Put(utils.GetTestKey(2), val2)
	assert.Nil(t, err)

	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	// 重复删除
	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	// 删除不存在的
	err = wb.Delete(utils.GetTestKey(3))
	assert.Nil(t, err)

	// 此时还未提交，两次Put都应该没有生效
	val, err := db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)

	val, err = db.Get(utils.GetTestKey(2))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)

	// 提交
	err = wb.Commit()
	assert.Nil(t, err)

	// 再次检查数据, 其中key1存在  key2被删除
	val, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, val1, val)

	val, err = db.Get(utils.GetTestKey(2))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)
}

func TestWriteBatch_Commit(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-writeBatch-put")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 初始化WriteBatch
	wb := db.NewWriteBatch(DefaultWriteBachOption)
	val1 := utils.GetTestRandomValue(10)
	err = wb.Put(utils.GetTestKey(1), val1)
	assert.Nil(t, err)

	val2 := utils.GetTestRandomValue(10)
	err = wb.Put(utils.GetTestKey(2), val2)
	assert.Nil(t, err)

	val3 := utils.GetTestRandomValue(10)
	err = wb.Put(utils.GetTestKey(3), val3)
	assert.Nil(t, err)

	err = wb.Delete(utils.GetTestKey(3))
	assert.Nil(t, err)

	// 此时还未提交，Put Delete都应该没有生效
	val, err := db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)

	val, err = db.Get(utils.GetTestKey(2))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)

	// 提交
	err = wb.Commit()
	assert.Nil(t, err)

	// 初始化事务2, 提交
	wb2 := db.NewWriteBatch(DefaultWriteBachOption)
	err = wb2.Put(utils.GetTestKey(5), utils.GetTestRandomValue(10))
	assert.Nil(t, err)

	err = wb2.Put(utils.GetTestKey(6), utils.GetTestRandomValue(10))
	assert.Nil(t, err)
	wb2.Commit()

	// 初始化事务3, 不提交
	wb3 := db.NewWriteBatch(DefaultWriteBachOption)
	err = wb3.Put(utils.GetTestKey(7), utils.GetTestRandomValue(10))
	assert.Nil(t, err)

	// 再次检查数据, val3已经删除
	val, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, val1, val)

	val, err = db.Get(utils.GetTestKey(2))
	assert.Equal(t, val2, val)

	val, err = db.Get(utils.GetTestKey(3))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)

	// 对DB进行重启
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	defer destroyDB(db2)
	assert.Nil(t, err)
	val, err = db2.Get(utils.GetTestKey(1))
	assert.Equal(t, val1, val)

	val, err = db2.Get(utils.GetTestKey(2))
	assert.Equal(t, val2, val)

	val, err = db2.Get(utils.GetTestKey(3))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)

	val, err = db2.Get(utils.GetTestKey(5))
	assert.NotNil(t, val)

	val, err = db2.Get(utils.GetTestKey(7))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)

	t.Log(db2.seqNo)
}
