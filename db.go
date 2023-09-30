package go_bitcask_kv

import (
	"errors"
	"go-bitcask-kv/data"
	"go-bitcask-kv/index"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask storage engine instance
type DB struct {
	option     Option
	mu         *sync.RWMutex
	activeFile *data.DataFile            // 当前活跃文件，可写入
	olderFiles map[uint32]*data.DataFile // immutable文件
	fileIds    []int                     // 用于fileId的排序
	index      index.Indexer             // 内存索引结构
}

// Open 打开存储引擎实例
func Open(option Option) (*DB, error) {
	// 参数校验
	if err := checkOptions(option); err != nil {
		return nil, err
	}

	// 目录校验，如果不存在则创建
	if _, err := os.Stat(option.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(option.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化DB结构体
	db := &DB{
		option:     option,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(option.IndexType),
	}

	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 从数据文件中加载索引 维护在内存中
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
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
	logRecord, _, err := dataFile.ReadLogRecord(recordPos.Offset)
	if err != nil {
		return nil, err
	}

	// 需要再判断类型
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 先查找key是否存在
	if recordPos := db.index.Get(key); recordPos == nil {
		return nil
	}

	// 构造墓碑追加写入到日志文件中
	logRecord := &data.LogRecord{
		Key:   key,
		Value: nil,
		Type:  data.LogRecordDeleted,
	}

	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 写入成功后从内存索引中删除
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// 参数校验
func checkOptions(option Option) error {
	if len(option.DirPath) == 0 {
		return errors.New("database dir path is empty")
	}

	if option.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}

	return nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	// 读取目录中的所有文件
	dirEntries, err := os.ReadDir(db.option.DirPath)
	if err != nil {
		return err
	}

	for _, entry := range dirEntries {
		if strings.HasPrefix(entry.Name(), data.DataFileNamePrefix) {
			// 文件名 bitcask_001.data
			spName := strings.Split(entry.Name(), ".")
			spNo := strings.Split(spName[0], "_")
			fileId, err := strconv.Atoi(spNo[1])
			if err != nil {
				return ErrDataDirNameIncorrect
			}

			db.fileIds = append(db.fileIds, fileId)
		}
	}

	// 对文件id排序，依次加载
	sort.Ints(db.fileIds)

	// 遍历文件id，打开所有的数据文件
	for i, fid := range db.fileIds {
		datafile, err := data.OpenDataFile(db.option.DirPath, uint32(fid))
		if err != nil {
			return nil
		}

		// 当遍历到最后一个文件，指定该文件为活跃文件
		if i == len(db.fileIds)-1 {
			db.activeFile = datafile
		} else {
			// 其他文件是older文件
			db.olderFiles[uint32(fid)] = datafile
		}
	}

	return nil
}

// 从数据文件中加载索引
// 遍历文件中的所有记录，并更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	// 如果当前fileIds为空，说明数据库为空，直接返回即可
	if len(db.fileIds) == 0 {
		return nil
	}

	// 遍历所有的文件id，处理文件中的记录
	for _, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		for {
			// 读取dataFile中的内容
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				// 判断是否是读完了
				if err == io.EOF {
					break
				}
				return err
			}

			// 将record插入到内存索引中
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}

			var ok bool
			if logRecord.Type == data.LogRecordDeleted {
				// 删除索引
				ok = db.index.Delete(logRecord.Key)
			} else {
				ok = db.index.Put(logRecord.Key, logRecordPos)
			}

			if !ok {
				return ErrIndexUpdateFailed
			}

			// offset加上记录长度
			offset += size
		}

		// 如果是活跃文件，要记录最后一个记录的最后位置
		// 因为追加写入的偏移需要记录
		if fileId == db.activeFile.FileId {
			db.activeFile.WriteOff = offset
		}
	}

	return nil
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

	return pos, nil
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
