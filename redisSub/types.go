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
// Key -> key
// Value -> type + expire + value

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

// ============================ HSet =============================
// key 					-> 	metadata(type expire version size)
// key+version+field  	-> 	value

// HSet field不存在返回true, 更新返回false
func (rds *RedisData) HSet(key, field, value []byte) (bool, error) {
	// 先查找元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	// 构造key-value
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()

	// 查找hk是否存在
	var exist = true
	if _, err := rds.db.Get(encKey); err == bitcask.ErrKeyNotFound {
		exist = false
	}

	// 要保证元数据和数据更新的一致性，使用writeBatch来更新
	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBachOption)

	// 如果hk不存在，那么说明是新插入的元素
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}

	// 不管是否存在，都需要更新value
	_ = wb.Put(encKey, value)

	if err = wb.Commit(); err != nil {
		return false, err
	}

	// 不存在返回true,更新返回false
	return !exist, nil
}

func (rds *RedisData) HGet(key, field []byte) ([]byte, error) {
	// 先找元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	// 再找数据
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	return rds.db.Get(hk.encode())
}

func (rds *RedisData) HDel(key, field []byte) (bool, error) {
	// 先找元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 再找数据
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	// 因为要返回是否删除成功，所以不能直接返回
	encKey := hk.encode()
	var exist = true
	if _, err := rds.db.Get(encKey); err == bitcask.ErrKeyNotFound {
		exist = false
	}

	if exist {
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBachOption)
		meta.size--

		// 因为没有进行实际的修改，不需要处理返回值
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)

		if err := wb.Commit(); err != nil {
			return false, err
		}
	}

	// 存在则返回true删除成功
	return exist, nil
}

// ============================ HSet =============================
// key 					-> 	metadata(type expire version size)
// key+version+member + member_size  	-> 	""
// 增加member_size是为了方便从末尾直接获取member元素

func (rds *RedisData) SAdd(key, member []byte) (bool, error) {
	// 先获取元数据
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	// 构造key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	encKey := sk.encode()

	var ok = false
	if _, err = rds.db.Get(encKey); err == bitcask.ErrKeyNotFound {
		// 只有不存在才做操作, 更新数据和元数据
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBachOption)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(encKey, nil)

		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil
}

func (rds *RedisData) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 构造一个数据部分的 key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = rds.db.Get(sk.encode())
	if err != nil && err != bitcask.ErrKeyNotFound {
		return false, err
	}
	if err == bitcask.ErrKeyNotFound {
		return false, nil
	}
	return true, nil
}

func (rds *RedisData) SRem(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 构造一个数据部分的 key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err = rds.db.Get(sk.encode()); err == bitcask.ErrKeyNotFound {
		return false, nil
	}

	// 更新
	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBachOption)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// ============================ List =============================
// 支持LPush LPop RPush RPop
// 实现上初始化head = tail = math.MaxUint64 / 2, 然后LPush对应head--, RPush对应tail++
// 或者使用循环队列即可实现
// key 					-> 	metadata(type expire version size head tail)
// key + version + index-> 	value

// ============================ ZSet =============================
// 支持ZAdd ZScore
// 因为bitcask内存索引是有序的, 会按照(key+version+score)排序，从迭代器中找出满足条件的score区间
// key 											-> 	metadata(type expire version size)
// key + version + member						-> 	score
// key + version + score + member + memberSize 	-> 	nil

// Close 关闭db实例
func (rds *RedisData) Close() error {
	return rds.db.Close()
}

// ============================ MetaData Operation =============================

func (rds *RedisData) findMetadata(key []byte, dataType redisDataType) (*metaData, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && err != bitcask.ErrKeyNotFound {
		return nil, err
	}

	var meta *metaData
	var exist = true
	if err == bitcask.ErrKeyNotFound {
		exist = false
	} else {
		meta = decodeMetadata(metaBuf)
		// 判断数据类型
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		// 判断过期时间
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		meta = &metaData{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}
