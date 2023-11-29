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

func TestRedisDataStructure_HGet(t *testing.T) {
	opts := bitcask.DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-hget")
	opts.DirPath = dir
	rds, err := NewRedisData(opts)
	assert.Nil(t, err)

	defer func() {
		_ = rds.db.Close()
		_ = os.RemoveAll(dir)
	}()

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.GetTestRandomValue(128))
	assert.Nil(t, err)
	assert.True(t, ok1)

	v1 := utils.GetTestRandomValue(128)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.Nil(t, err)
	assert.False(t, ok2)

	v2 := utils.GetTestRandomValue(128)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.Nil(t, err)
	assert.True(t, ok3)

	val1, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.Equal(t, v1, val1)

	val2, err := rds.HGet(utils.GetTestKey(1), []byte("field2"))
	assert.Nil(t, err)
	assert.Equal(t, v2, val2)

	_, err = rds.HGet(utils.GetTestKey(1), []byte("field-not-exist"))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)
}

func TestRedisDataStructure_HDel(t *testing.T) {
	opts := bitcask.DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-hdel")
	opts.DirPath = dir
	rds, err := NewRedisData(opts)
	assert.Nil(t, err)

	defer func() {
		_ = rds.db.Close()
		_ = os.RemoveAll(dir)
	}()

	del1, err := rds.HDel(utils.GetTestKey(200), nil)
	assert.Nil(t, err)
	assert.False(t, del1)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.GetTestRandomValue(128))
	assert.Nil(t, err)
	assert.True(t, ok1)

	v1 := utils.GetTestRandomValue(128)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.Nil(t, err)
	assert.False(t, ok2)

	v2 := utils.GetTestRandomValue(128)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.Nil(t, err)
	assert.True(t, ok3)

	del2, err := rds.HDel(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.True(t, del2)
}

func TestRedisDataStructure_SAdd_SIsMember(t *testing.T) {
	opts := bitcask.DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-SAdd")
	opts.DirPath = dir
	rds, err := NewRedisData(opts)
	assert.Nil(t, err)

	defer func() {
		_ = rds.db.Close()
		_ = os.RemoveAll(dir)
	}()

	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(2), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-not-exist"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestRedisDataStructure_SRem(t *testing.T) {
	opts := bitcask.DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-SRem")
	opts.DirPath = dir
	rds, err := NewRedisData(opts)
	assert.Nil(t, err)

	defer func() {
		_ = rds.db.Close()
		_ = os.RemoveAll(dir)
	}()

	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SRem(utils.GetTestKey(2), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SRem(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.False(t, ok)
}
