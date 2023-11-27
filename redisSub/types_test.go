package redisSub

import (
	"github.com/stretchr/testify/assert"
	bitcask "go-bitcask-kv"
	"go-bitcask-kv/utils"
	"os"
	"testing"
	"time"
)

func TestRedisData_Set_Get(t *testing.T) {
	opts := bitcask.DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.DirPath = dir
	rds, err := NewRedisData(opts)
	assert.Nil(t, err)

	defer func() {
		_ = rds.db.Close()
		_ = os.RemoveAll(dir)
	}()

	err = rds.Set(utils.GetTestKey(1), 0, []byte("hello world"))
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), time.Second*2, []byte("redis"))
	assert.Nil(t, err)

	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, []byte("hello world"), val1)

	val2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.Equal(t, []byte("redis"), val2)

	time.Sleep(time.Second * 2)

	val3, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.Nil(t, val3)
}

func TestRedisDataStructure_Del_Type(t *testing.T) {
	opts := bitcask.DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-del-type")
	opts.DirPath = dir
	rds, err := NewRedisData(opts)
	assert.Nil(t, err)

	defer func() {
		_ = rds.db.Close()
		_ = os.RemoveAll(dir)
	}()

	// del
	err = rds.Del(utils.GetTestKey(11))
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.GetTestRandomValue(128))
	assert.Nil(t, err)

	// type
	typ, err := rds.Type(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, String, typ)

	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	_, err = rds.Get(utils.GetTestKey(1))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)
}
