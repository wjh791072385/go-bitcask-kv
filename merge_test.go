package bitcaskKV

import (
	"github.com/stretchr/testify/assert"
	"go-bitcask-kv/utils"
	"os"
	"sync"
	"testing"
	"time"
)

// 测试空数据 以及 小数据的情况
func TestDB_Merge1(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-merge1")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 直接merge，返回nil
	err = db.Merge()
	assert.Nil(t, err)

	// 写一小部分数据
	for i := 0; i < 100; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestRandomValue(128))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Equal(t, ErrMergeCondUnreached, err)
}

// 测试全是有效数据，无法达到merge要求
func TestDB_Merge2(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-merge2")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer func() {
		_ = os.RemoveAll(db.option.DirPath)
	}()
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 写入大约130MB数据
	for i := 0; i < 800000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestRandomValue(128))
		assert.Nil(t, err)
	}
	//t.Log(db.Stat().DiskSize / 1024 / 1024)

	// 因为全部是有效数据，因此不会出发merge操作
	err = db.Merge()
	assert.Equal(t, ErrMergeCondUnreached, err)

	//// 重启校验
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	defer func() {
		_ = db2.Close()
	}()
	stat := db2.Stat()
	assert.Equal(t, uint32(800000), stat.KeyNum)
	//t.Log(stat)

	for i := 0; i < 800000; i++ {
		_, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
}

// 部分失效数据，达到merge要求
func TestDB_Merge3(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-merge3")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer func() {
		_ = os.RemoveAll(db.option.DirPath)
	}()
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 写入数据
	for i := 0; i < 2000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestRandomValue(128))
		assert.Nil(t, err)
	}
	// 数据总大小326MB
	t.Log(db.Stat().DiskSize / 1024 / 1024)

	// 重复put数据，删除数据
	for i := 0; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestRandomValue(128))
		assert.Nil(t, err)
	}

	for i := 0; i < 500000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}

	_, err = db.Get(utils.GetTestKey(500001))
	assert.Nil(t, err)

	// 数据总大小503MB
	//t.Log(db.Stat().DiskSize / 1024 / 1024)

	// 可回收数据大小258MB
	//t.Log(db.RecycleSize / 1024 / 1024)

	// 执行数据Merge操作
	err = db.Merge()
	assert.Nil(t, err)

	// 重启校验
	err = db.Close()
	assert.Nil(t, err)

	// 之前有defer函数销毁
	db2, err := Open(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)

	// 应该剩余1500000个key有效
	assert.Equal(t, uint32(1500000), db2.Stat().KeyNum)
	//t.Log(db.Stat().DiskSize / 1024 / 1024)

	for i := 500000; i < 2000000; i++ {
		_, err = db2.Get(utils.GetTestKey(500001))
		assert.Nil(t, err)
	}
}

// 测试merge的过程中有新的数据写入或者删除
func TestDB_Merge4(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-merge3")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer func() {
		_ = os.RemoveAll(db.option.DirPath)
	}()
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 构造冗余数据，触发merge阈值
	for i := 0; i < 2000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestRandomValue(128))
		assert.Nil(t, err)
	}

	// 重复put数据，删除数据
	for i := 0; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestRandomValue(128))
		assert.Nil(t, err)
	}

	for i := 0; i < 500000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}

	//t.Log(db.Stat())

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// 先让merge获取锁，执行merge操作
		time.Sleep(2 * time.Second)
		//t.Log("begin to write while merging, activeFid = ", db.activeFile.FileId)
		for i := 2000000; i < 2800000; i++ {
			err := db.Put(utils.GetTestKey(i), utils.GetTestRandomValue(128))
			assert.Nil(t, err)
		}
		for i := 1850000; i < 2350000; i++ {
			err := db.Delete(utils.GetTestKey(i))
			assert.Nil(t, err)
		}
	}()
	err = db.Merge()
	assert.Nil(t, err)
	wg.Wait()

	// 重启校验
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)

	// 写入200w，删除50w, 写入80w，删除50w，最终剩余180w
	assert.Equal(t, uint32(180*10000), db2.Stat().KeyNum)
	//t.Log(db2.Stat())

	for i := 500000; i < 1000000; i++ {
		_, err = db2.Get(utils.GetTestKey(500001))
		assert.Nil(t, err)
	}
}
