package bitcaskKV

import (
	"encoding/binary"
	"go-bitcask-kv/data"
	"sync"
	"sync/atomic"
)

// WriteBatch 原子批量写数据
type WriteBatch struct {
	option WriteBatchOption
	mu     *sync.Mutex
	db     *DB

	// 暂存用户写入的数据
	pendingWrites map[string]*data.LogRecord
}

var txnFish = []byte("txn-fin")

const nonTransactionSeqNo = 0

// NewWriteBatch 初始化WriteBatch
func (db *DB) NewWriteBatch(opt WriteBatchOption) *WriteBatch {
	return &WriteBatch{
		option:        opt,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 批量写数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 不实际进行写操作，而是暂存起来
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Delete 批量删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 如果不存在直接返回, 并且把批处理中的该key对应的写都删除掉
	pos := wb.db.index.Get(key)
	if pos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// 不实际进行写操作，而是暂存起来
	logRecord := &data.LogRecord{
		Key:   key,
		Value: nil,
		Type:  data.LogRecordDeleted,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if len(wb.pendingWrites) > wb.option.maxBatchNum {
		return ErrExceedMaxBatchNum
	}

	// 对数据库加锁，保证提交操作的串行化
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// 获取事务id
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// 暂存索引信息，成功后统一更新
	positions := make(map[string]*data.LogRecordPos)

	// 将数据写入到日志文件中
	for _, record := range wb.pendingWrites {
		// 不能直接使用Put, Put操作会更新索引信息
		//err := wb.db.Put(record.Key, record.Value)
		pos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   encodeRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})

		if err != nil {
			return err
		}

		positions[string(record.Key)] = pos
	}

	// 追加一条标识事务完成的数据
	finishedRecord := &data.LogRecord{
		Key:   encodeRecordKeyWithSeq(txnFish, seqNo),
		Value: nil,
		Type:  data.LogRecordTxnFinished,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return nil
	}

	// 此时表示事务已经完成，根据配置持久化
	if wb.option.SyncWriteBatch && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		var oldValue *data.LogRecordPos
		if record.Type == data.LogRecordNormal {
			oldValue, _ = wb.db.index.Put(record.Key, pos)
		}

		if record.Type == data.LogRecordDeleted {
			oldValue, _ = wb.db.index.Delete(record.Key)
		}
		if oldValue != nil {
			wb.db.recycleSize += oldValue.Size
		}
	}

	// 清空暂存数据，防止重复使用同一个writeBatch
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// 把序号编码进key中
func encodeRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)
	return encKey
}

// 解析出序号和key
func decodeRecordKeyWithSeq(buf []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(buf)
	realKey := buf[n:]
	return realKey, seqNo
}
