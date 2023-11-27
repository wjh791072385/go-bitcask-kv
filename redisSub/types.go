package redisSub

import (
	"encoding/binary"
	"errors"
	bitcask "go-bitcask-kv"
	"time"
)

var (
	ErrWrongTypeOperation = errors.New("wrong type operation against a key holding the wrong kind of value")
)

type redisDataType byte

const (
	String redisDataType = iota + 1
	Hash
	Set
	List
	ZSet
)

type RedisData struct {
	db *bitcask.DB
}

func NewRedisData(option bitcask.Option) (*RedisData, error) {
	db, err := bitcask.Open(option)
	if err != nil {
		return nil, err
	}
	return &RedisData{
		db: db,
	}, nil
}

// ============================ String =============================
// Key : key
// Value : type + expire + value

func (rds *RedisData) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	var index = 0
	buf := make([]byte, binary.MaxVarintLen64+1+len(value))

	// 第一个字节存储type
	buf[0] = byte(String)
	index += 1

	// 然后存放过期时间
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)

	// 最后存储实际value
	copy(buf[index:], value)
	index += len(value)

	return rds.db.Put(key, buf[0:index])
}

func (rds *RedisData) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	// 对encValue进行解码
	var index = 0
	var dataType redisDataType = redisDataType(encValue[0])
	index += 1
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}

	// 判断过期时间，过期则返回nil
	expire, n := binary.Varint(encValue[index:])
	index += n
	if expire > 0 && expire < time.Now().UnixNano() {
		return nil, nil
	}

	return encValue[index:], nil
}
