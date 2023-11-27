package benchmark

import (
	"github.com/stretchr/testify/assert"
	bitcask "go-bitcask-kv"
	"go-bitcask-kv/utils"
	"math/rand"
	"os"
	"testing"
	"time"
)

var db *bitcask.DB
var dbPath string

func dbInit() {
	opts := bitcask.DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-benchmark")
	opts.DirPath = dir
	dbPath = dir
	opts.DataFileSize = 64 * 1024 * 1024

	var err error
	db, err = bitcask.Open(opts)
	if err != nil {
		panic(err)
	}
}

func Benchmark_Put(b *testing.B) {
	dbInit()
	defer func() {
		err := db.Close()
		if err != nil {
			panic("db close failed")
		}
		_ = os.RemoveAll(dbPath)
	}()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestRandomValue(512))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	dbInit()
	defer func() {
		err := db.Close()
		if err != nil {
			panic("db close failed")
		}
		_ = os.RemoveAll(dbPath)
	}()

	// 先加入一些数据
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestRandomValue(512))
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(rand.Int() % 30000))
		if err != nil && err != bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	dbInit()
	defer func() {
		err := db.Close()
		if err != nil {
			panic("db close failed")
		}
		_ = os.RemoveAll(dbPath)
	}()

	// 先加入一些数据
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestRandomValue(512))
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(rand.Int() % 30000))
		if err != nil {
			b.Fatal(err)
		}
	}
}
