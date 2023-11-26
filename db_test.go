package bitcaskKV

import (
	"github.com/stretchr/testify/assert"
	"go-bitcask-kv/utils"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 测试完成之后销毁 DB 数据目录
func destroyDB(db *DB) {
	if db != nil {
		err := db.Close()
		if err != nil {
			panic(err)
		}
		err = os.RemoveAll(db.option.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestGoFunction(t *testing.T) {
	fp := "/root/wjh/hello.txt"
	t.Log(filepath.Base(fp))
	t.Log(filepath.Dir(fp))
	t.Log(filepath.Abs(fp))
}

func TestOpen(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_FileLock(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 再次打开DB会报错
	db2, err := Open(opts)
	assert.Nil(t, db2)
	assert.NotNil(t, err)

	// 关闭后再次打开可以
	err = db.Close()
	assert.Nil(t, err)
	db2, err = Open(opts)
	defer destroyDB(db2)
	assert.Nil(t, err)
	assert.NotNil(t, db2)

	err = db2.Close()
	assert.Nil(t, err)
}

func TestDB_Put(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-put")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常 Put 一条数据
	err = db.Put(utils.GetTestKey(1), utils.GetTestRandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.重复 Put key 相同的数据
	err = db.Put(utils.GetTestKey(1), utils.GetTestRandomValue(24))
	assert.Nil(t, err)
	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	// 3.key 为空
	err = db.Put(nil, utils.GetTestRandomValue(24))
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4.value 为空
	err = db.Put(utils.GetTestKey(22), nil)
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Equal(t, 0, len(val3))
	assert.Nil(t, err)

	// 5.写到数据文件进行了转换
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestRandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderFiles))

	// 6.重启后再 Put 数据
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val4 := utils.GetTestRandomValue(128)
	err = db2.Put(utils.GetTestKey(55), val4)
	assert.Nil(t, err)
	val5, err := db2.Get(utils.GetTestKey(55))
	assert.Nil(t, err)
	assert.Equal(t, val4, val5)
}

func TestDB_Get(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-get")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常读取一条数据
	err = db.Put(utils.GetTestKey(11), utils.GetTestRandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.读取一个不存在的 key
	val2, err := db.Get([]byte("some key unknown"))
	assert.Nil(t, val2)
	assert.Equal(t, ErrKeyNotFound, err)

	// 3.值被重复 Put 后在读取
	err = db.Put(utils.GetTestKey(22), utils.GetTestRandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.GetTestRandomValue(24))
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val3)

	// 4.值被删除后再 Get
	err = db.Put(utils.GetTestKey(33), utils.GetTestRandomValue(24))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(33))
	assert.Nil(t, err)
	val4, err := db.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val4))
	assert.Equal(t, ErrKeyNotFound, err)

	// 5.转换为了旧的数据文件，从旧的数据文件上获取 value
	for i := 100; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestRandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderFiles))
	val5, err := db.Get(utils.GetTestKey(101))
	assert.Nil(t, err)
	assert.NotNil(t, val5)

	//6.重启后，前面写入的数据都能拿到
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	val6, err := db2.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val6)
	assert.Equal(t, val1, val6)

	val7, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val7)
	assert.Equal(t, val3, val7)

	val8, err := db2.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val8))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_Delete(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-delete")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常删除一个存在的 key
	err = db.Put(utils.GetTestKey(11), utils.GetTestRandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(11))
	assert.Nil(t, err)
	_, err = db.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	// 2.删除一个不存在的 key
	err = db.Delete([]byte("unknown key"))
	assert.Nil(t, err)

	// 3.删除一个空的 key
	err = db.Delete(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4.值被删除之后重新 Put
	err = db.Put(utils.GetTestKey(22), utils.GetTestRandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(22))
	assert.Nil(t, err)

	err = db.Put(utils.GetTestKey(22), utils.GetTestRandomValue(128))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(22))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// 5.重启之后，再进行校验
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	_, err = db2.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	val2, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.Equal(t, val1, val2)
}

func TestDB_ListKeys(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-list-keys")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 数据库为空
	keys1 := db.ListKeys()
	assert.Equal(t, 0, len(keys1))

	// 只有一条数据
	err = db.Put(utils.GetTestKey(11), utils.GetTestRandomValue(20))
	assert.Nil(t, err)
	keys2 := db.ListKeys()
	assert.Equal(t, 1, len(keys2))

	// 有多条数据
	err = db.Put(utils.GetTestKey(22), utils.GetTestRandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(33), utils.GetTestRandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(44), utils.GetTestRandomValue(20))
	assert.Nil(t, err)

	keys3 := db.ListKeys()
	assert.Equal(t, 4, len(keys3))
	for _, k := range keys3 {
		//t.Log(k)
		assert.NotNil(t, k)
	}
}

func TestDB_Fold(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-fold")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.GetTestRandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.GetTestRandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(33), utils.GetTestRandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(44), utils.GetTestRandomValue(20))
	assert.Nil(t, err)

	res := make([][]byte, 0)
	err = db.Fold(func(key []byte, value []byte) bool {
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		res = append(res, key) // 获取key，读取到外面
		return true
	})
	assert.Equal(t, 4, len(res))
	assert.Nil(t, err)
}

func TestDB_Close(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-close")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.GetTestRandomValue(20))
	assert.Nil(t, err)
}

func TestDB_Sync(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-sync")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.GetTestRandomValue(20))
	assert.Nil(t, err)

	err = db.Sync()
	assert.Nil(t, err)
}

func TestDB_LoadDataFilesByMMap(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-MMap")
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			panic(err)
		}
	}()
	opts.DirPath = dir

	// 64MB一个日志文件
	opts.DataFileSize = 64 * 1024 * 1024

	// 先使用标准IO写数据
	opts.MMapAtStartup = false
	db, err := Open(opts)
	assert.Nil(t, err)

	// 一条数据假定128B
	// 128 * 1024 * 1024 = 128M
	for i := 0; i < 1024*1024; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestRandomValue(128))
		assert.Nil(t, err)
	}

	err = db.Close()
	assert.Nil(t, err)

	// 正常启动加载数据
	startT := time.Now()
	opts.MMapAtStartup = false
	db, err = Open(opts)
	cost1 := time.Since(startT)
	t.Log(cost1)
	err = db.Close()
	assert.Nil(t, err)

	// 使用MMap加载数据
	startT = time.Now()
	opts.MMapAtStartup = false
	db, err = Open(opts)
	cost2 := time.Since(startT)
	t.Log(cost2)
	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_Stat(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-stat")
	opts.DirPath = dir

	// 64MB一个日志文件
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 写入一部分数据
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestRandomValue(128))
		assert.Nil(t, err)
	}

	stat := db.Stat()
	assert.NotNil(t, stat)
	//t.Log(stat)

	// 再重复写入，相当于之前的key都将变为无效
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestRandomValue(128))
		assert.Nil(t, err)
	}

	//stat = db.Stat()
	//t.Log(stat)

	// 在删除一部分
	for i := 0; i < 5000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	//stat = db.Stat()
	//t.Log(stat)
}

func TestDB_Backup(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-go-backup")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 1; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestRandomValue(128))
		assert.Nil(t, err)
	}

	backupDir, _ := os.MkdirTemp("", "bitcask-go-backup-test")
	err = db.Backup(backupDir)
	assert.Nil(t, err)

	opts1 := DefaultOption
	opts1.DirPath = backupDir
	db2, err := Open(opts1)
	defer destroyDB(db2)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
}
