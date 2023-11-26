package bitcaskKV

import (
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"go-bitcask-kv/data"
	"go-bitcask-kv/fio"
	"go-bitcask-kv/index"
	"go-bitcask-kv/utils"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask storage engine instance
// It is also a log-structure storage
type DB struct {
	option Option
	mu     *sync.RWMutex

	// active file can be written
	activeFile *data.SegDataFile

	// older files are immutable. It maintains FileId to log
	olderFiles map[uint32]*data.SegDataFile

	// fileIds is used for sorting
	fileIds []int

	index index.Indexer

	// WriteBatch Sequence Number
	seqNo uint64

	// when true, the DB is merging.
	// Only one Merge operation can run at a time
	isMerging bool

	// when ture, the DB is successfully started
	isInitial bool

	// file lock ensure one DB instance processing
	fileLock *flock.Flock

	// the number of bytes written, but has not been persistent
	bytesWrite uint

	// invalid data size, need to be merged
	recycleSize uint32
}

type Stat struct {
	// 数据文件数量
	DataFilNum uint32

	// key的数量
	KeyNum uint32

	// 无效数据
	RecycleSize uint32

	// 占用磁盘空间大小
	DiskSize uint64
}

// Open creates and opens a DB instance with specified option
func Open(option Option) (*DB, error) {
	// option check
	if err := checkOptions(option); err != nil {
		return nil, err
	}

	// check directory if exists.
	// if not exists, create it.
	if _, err := os.Stat(option.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(option.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// try to get file lock
	fileLock := flock.New(filepath.Join(option.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}

	// 如果获取不到说明已经有其他的DB实例占用了该目录
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	// Initialize DB
	db := &DB{
		option:      option,
		mu:          new(sync.RWMutex),
		olderFiles:  make(map[uint32]*data.SegDataFile),
		index:       index.NewIndexer(option.IndexType, option.indexPath),
		fileLock:    fileLock,
		recycleSize: 0,
	}

	// load MergeFiles
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// load DataFiles and open the data file pointer
	// use memory map IO
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// Only the memory index need to load index file
	// the B+Tree index is persistent, maintain the persistent index by itself
	// If choose B+Tree persistent index, need to get the seqNo(transaction serial number)
	if db.option.IndexType != index.BPlusTreeIndex {
		// Firstly, load index from hintFile
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		// Secondly, load index from DateFiles(never be merged)
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}
	}

	// 如果使用MMap加载数据文件
	// 读取完之后重置为标准IO, MMap仅仅用于数据加载
	if db.option.MMapAtStartup {
		if err := db.resetIOType(fio.StandardIO); err != nil {
			return nil, err
		}
	}

	db.isInitial = true

	return db, nil
}

func (db *DB) Stat() *Stat {
	db.mu.Lock()
	defer db.mu.Unlock()

	var dataFileNum = len(db.olderFiles)
	if db.activeFile != nil {
		dataFileNum += 1
	}

	totalSize, err := utils.DirSize(db.option.DirPath)
	if err != nil {
		return nil
	}

	return &Stat{
		DataFilNum:  uint32(dataFileNum),
		KeyNum:      uint32(db.index.Size()),
		RecycleSize: db.recycleSize,
		DiskSize:    uint64(totalSize),
	}
}

// Backup 数据备份到指定目录
func (db *DB) Backup(dir string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return utils.CopyDir(db.option.DirPath, dir, []string{fileLockName})
}

// Put 写入key-value，key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 单条日志记录
	logRecord := &data.LogRecord{
		Key:   encodeRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	// 如果已经原来已经有该key了，说明之前的数据就无效了，递增无效值
	if oldValue, _ := db.index.Put(key, pos); oldValue != nil {
		db.recycleSize += oldValue.Size
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

	// 根据索引信息找到对应记录
	return db.getValueByPosition(recordPos)
}

func (db *DB) getValueByPosition(recordPos *data.LogRecordPos) ([]byte, error) {
	// 根据文件id找到文件，先尝试从active文件中找，再在旧文件中找
	var dataFile *data.SegDataFile
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

// ListKeys 获取存储引擎中
func (db *DB) ListKeys() [][]byte {
	keys := make([][]byte, db.index.Size())
	it := db.index.Iterator(false)

	// 通过迭代器获取keys
	idx := 0
	for it.Rewind(); it.Valid(); it.Next() {
		keys[idx] = it.Key()
		idx++
	}

	return keys
}

type handleFunc func(key []byte, value []byte) bool

// Fold 对DB中的所有数据执行指定操作
func (db *DB) Fold(hd handleFunc) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	it := db.index.Iterator(false)
	for it.Rewind(); it.Valid(); it.Next() {
		val, err := db.getValueByPosition(it.Value())
		if err != nil {
			return err
		}

		// 执行回调函数
		if hd(it.Key(), val) == false {
			break
		}
	}
	return nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 先查找key是否存在
	if recordPos := db.index.Get(key); recordPos == nil {
		// 不存在的话应该返回nil ，毕竟也是正确删除，并非发生错误
		return nil
	}

	// 构造墓碑追加写入到日志文件中
	logRecord := &data.LogRecord{
		Key:   encodeRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: nil,
		Type:  data.LogRecordDeleted,
	}

	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 该条删除记录也是可以回收的
	db.recycleSize += pos.Size

	// 写入成功后从内存索引中删除
	oldValue, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}

	// 将之前的记录删除，叠加回收值
	if oldValue != nil {
		db.recycleSize += oldValue.Size
	}
	return nil
}

func (db *DB) Close() error {
	// 在最后释放文件锁
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory %s and error is %v", db.option.DirPath, err))
		}
	}()

	if db.activeFile == nil {
		// 说明都没启动
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// 索引close 目前只针对B+树
	if err := db.index.Close(); err != nil {
		return err
	}

	// 关闭活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// 关闭旧数据文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Sync 刷盘 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		// 说明都没启动
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.activeFile.Sync()
}

// 参数校验
func checkOptions(option Option) error {
	if len(option.DirPath) == 0 {
		return errors.New("database dir path is empty")
	}

	if option.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}

	if option.mergeRatioThr <= 0 || option.mergeRatioThr >= 1 || option.mergeMinSizeThr < 0 {
		return errors.New("merge threshold option is invalid")
	}

	return nil
}

// 从磁盘中加载数据文件对应指针
func (db *DB) loadDataFiles() error {
	// 读取目录中的所有文件
	dirEntries, err := os.ReadDir(db.option.DirPath)
	if err != nil {
		return err
	}

	for _, entry := range dirEntries {
		if strings.HasPrefix(entry.Name(), data.SegDataFileNamePrefix) && strings.HasSuffix(entry.Name(), data.SegDataFileNameSuffix) {
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

	ioType := fio.StandardIO
	if db.option.MMapAtStartup {
		ioType = fio.MemoryIO
	}

	// 遍历文件id，打开所有的数据文件
	for i, fid := range db.fileIds {
		datafile, err := data.OpenDataFile(db.option.DirPath, uint32(fid), ioType)
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

// 更新内存索引
// 从数据文件中加载索引
// 遍历文件中的所有记录，并更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	// 如果当前fileIds为空，说明数据库为空，直接返回即可
	if len(db.fileIds) == 0 {
		return nil
	}

	// 判断是否发生过merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinishedFileName := filepath.Join(db.option.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinishedFileName); err == nil {
		// 如果存在则置为true
		fid, err := db.getNonMergeFileId(db.option.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	// 更新内存索引辅助函数
	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var oldValue *data.LogRecordPos
		if typ == data.LogRecordDeleted {
			oldValue, _ = db.index.Delete(key)
			// 删除数据这条记录本身也可以回收
			db.recycleSize += pos.Size
		} else if typ == data.LogRecordNormal {
			oldValue, _ = db.index.Put(key, pos)
		}

		// 更新可以回收的空间大小
		if oldValue != nil {
			db.recycleSize += oldValue.Size
		}
	}

	// 暂存事务数据
	transactionRecord := make(map[uint64][]*data.TransactionRecord)

	// 记录最大的序列号
	var currentSeqNo uint64 = nonTransactionSeqNo

	// 遍历所有的文件id，处理文件中的记录
	for _, fid := range db.fileIds {
		var fileId = uint32(fid)

		// 如果加载过hint文件，则跳过已经merge过的文件
		if hasMerge && fileId < nonMergeFileId {
			continue
		}

		var dataFile *data.SegDataFile
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

			// 构造内存索引
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
				Size:   uint32(size),
			}

			// 解析key，判断是否是事务
			realKey, seqNo := decodeRecordKeyWithSeq(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				// 非事务操作，直接更新
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// 针对事务，先将其缓存起来，直到读取到事务结束标志
				if logRecord.Type == data.LogRecordTxnFinished {
					// 更新对应的索引
					for _, rec := range transactionRecord[seqNo] {
						updateIndex(rec.Record.Key, rec.Record.Type, rec.Pos)
					}

					// 删除缓存
					delete(transactionRecord, seqNo)
				} else {
					logRecord.Key = realKey
					transactionRecord[seqNo] = append(transactionRecord[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
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

	// 更新事务序列号
	db.seqNo = currentSeqNo

	return nil
}

// 正常put delete需要加锁
func (db *DB) appendLogRecordWithLock(record *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.appendLogRecord(record)
}

// 追加写入日志文件中，方便writeBatch时不加锁
func (db *DB) appendLogRecord(record *data.LogRecord) (*data.LogRecordPos, error) {
	// 确保活跃文件存在
	if db.activeFile == nil {
		if err := db.setNewActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 将数据进行编码
	encRecord, size := data.EncodeLogRecord(record)

	// 如果超过了文件阈值，active文件转为older文件，新建一个active文件
	if db.activeFile.WriteOff+size > db.option.DataFileSize {
		// 当前活跃文件刷盘
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// active -> older
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打开一个新的活跃文件
		if err := db.setNewActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 执行实际数据写入, writeOff记录数据的的开始位置
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 累积每次写入的字节数
	db.bytesWrite += uint(size)

	// 持久化判断, 初始化为配置项
	needSync := db.option.SyncWrites

	// 不是每次持久化，而是根据累积字节数持久化
	// 这里后续可以抽象出一个方法，持久化策略选择
	if !needSync && db.option.BytesPerSync > 0 && db.bytesWrite >= db.option.BytesPerSync {
		needSync = true
	}

	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	// 构建内存索引信息，返回出去
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
		Size:   uint32(size),
	}

	return pos, nil
}

// 初始化当前活跃文件
// 访问该方法前必须持有db互斥锁
func (db *DB) setNewActiveDataFile() error {
	var initialFileId uint32 = 0

	if db.activeFile != nil {
		// 保持递增
		initialFileId = db.activeFile.FileId + 1
	}

	dataFile, err := data.OpenDataFile(db.option.DirPath, initialFileId, fio.StandardIO)
	if err != nil {
		return err
	}

	db.activeFile = dataFile
	return nil
}

func (db *DB) resetIOType(ioType fio.IOType) error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.option.DirPath, ioType); err != nil {
		return err
	}

	for _, datafile := range db.olderFiles {
		if err := datafile.SetIOManager(db.option.DirPath, ioType); err != nil {
			return err
		}
	}
	return nil
}
