package bitcaskKV

import (
	"github.com/stretchr/testify/assert"
	"go-bitcask-kv/utils"
	"os"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-iterator")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("hello"), utils.GetTestRandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("hel"), utils.GetTestRandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("world"), utils.GetTestRandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bitcask"), utils.GetTestRandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("hh"), utils.GetTestRandomValue(10))
	assert.Nil(t, err)

	// 正向迭代
	iter1 := db.NewIterator(DefaultIteratorOption)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}
	iter1.Rewind()
	for iter1.Seek([]byte("w")); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}
	iter1.Close()

	// 反向迭代
	iterOpts1 := DefaultIteratorOption
	iterOpts1.reverse = true
	iter2 := db.NewIterator(iterOpts1)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}
	iter2.Rewind()
	for iter2.Seek([]byte("w")); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}
	iter2.Close()

	// 指定了 prefix
	iterOpts2 := DefaultIteratorOption
	iterOpts2.prefix = []byte("hel")
	iter3 := db.NewIterator(iterOpts2)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}
	iter3.Close()
}
