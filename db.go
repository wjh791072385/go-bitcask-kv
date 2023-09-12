package go_bitcask_kv

import (
	"go-bitcask-kv/data"
	"go-bitcask-kv/index"
	"sync"
)

// DB bitcask storage engine instance
type DB struct {
	option     Option
	mu         *sync.RWMutex
	activeFile *data.DataFile            // 当前活跃文件，可写入
	olderFiles map[uint32]*data.DataFile // immutable文件
	index      index.Indexer             // 内存索引结构
}

// Put 写入key-value，key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 单条日志记录
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存结构中获取key的索引信息
	recordPos := db.index.Get(key)
	if recordPos == nil {
		return nil, ErrKeyNotFound
	}

	// 根据文件id找到文件，先尝试从active文件中找，再在旧文件中找
	var dataFile *data.DataFile
	if db.activeFile.FileId == recordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[recordPos.Fid]
	}

	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移量读取数据

}

// 追加写入日志文件中
func (db *DB) appendLogRecord(record *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 确保活跃文件存在
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 将数据进行编码
	encRecord, size := data.EncodeLogRecord(record)

	// 如果超过了文件阈值，active文件转为older文件，新建一个active文件
	if db.activeFile.WriteOff+size > db.option.DataFileSize {
		// 活跃文件刷盘
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// active -> older
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打开一个新的活跃文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 执行实际数据写入, writeOff记录数据的的开始位置
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 根据配置判断对单次写入是否持久化
	// 一般都是后续批量持久化, 即no-force
	if db.option.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 构建内存索引信息，返回出去
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
	}

	return pos
}

// 初始化当前活跃文件
// 访问该方法前必须持有db互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0

	if db.activeFile != nil {
		// 保持递增
		initialFileId = db.activeFile.FileId + 1
	}

	dataFile, err := data.OpenDataFile(db.option.DirPath, initialFileId)
	if err != nil {
		return err
	}

	db.activeFile = dataFile
	return nil
}
