package bitcaskKV

import (
	"go-bitcask-kv/data"
	"go-bitcask-kv/utils"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge_finished"
)

func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	// 判断DB是否正在merge
	// 不需要使用defer来释放锁，因为merge并不是全过程都需要锁
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsRunning
	}

	// 判断当前系统是否达到可以merge的阈值
	need, err := db.needMerge()
	if err != nil {
		// 别忘了释放锁
		db.mu.Unlock()
		return err
	}

	if !need {
		db.mu.Unlock()
		return ErrMergeCondUnreached
	}

	//fmt.Println("merge is begin")

	db.isMerging = true
	defer func() {
		// 在最后改为false
		db.isMerging = false
	}()

	// 持久化当前文件，并且重新开启一个新的active文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	db.olderFiles[db.activeFile.FileId] = db.activeFile
	if err := db.setNewActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}

	// 记录最新的active文件id, 表示之前的都已经参与merge操作了
	nonMergeFileId := db.activeFile.FileId

	// 取出所有需要Merge的文件，之后就能释放锁了
	// DB可以继续接收用户新的写入请求
	var mergeFiles []*data.SegDataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()

	// 对需要merge的文件进行排序，因为map是无序的
	// 从小到大去merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := db.getMergePath()

	// 如果存在目录，说明之前可能已经merge过，进行删除
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	// 新建merge目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	// 重新打开一个bitcask实例去merge
	mergeOption := db.option
	mergeOption.DirPath = mergePath
	mergeOption.SyncWrites = false
	mergeDB, err := Open(mergeOption)
	if err != nil {
		return err
	}

	// 打开一个hint文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)

	// 遍历处理每个数据文件
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}

				return err
			}

			realKey, _ := decodeRecordKeyWithSeq(logRecord.Key)
			logRecordPos := db.index.Get(realKey)

			// 将读取出的数据和内存中的数据比较，如果一致则说明该数据是有效的
			// 因为内存中的数据是最新的
			// 如果是事务，那也是已经commit并且成功的事务才会更新到内存中
			if logRecordPos != nil && logRecordPos.Fid == dataFile.FileId && logRecordPos.Offset == offset {
				// 清除事务标记，因为都是有效的key
				logRecord.Key = encodeRecordKeyWithSeq(realKey, nonTransactionSeqNo)

				// 重写数据，写入到merge目录中
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return nil
				}

				// 记录hint文件，其实就是记录索引信息，pos大小一般比value会小
				// !!这里注意hint文件不需要记录事务序列号，存储realKey
				hintRecord := &data.LogRecord{
					Key:   realKey,
					Value: data.EncodeLogRecordPos(pos),
				}

				enc, _ := data.EncodeLogRecord(hintRecord)
				if err := hintFile.Write(enc); err != nil {
					return nil
				}
			}

			offset += size
		}
	}

	// 持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}

	if err := mergeDB.Sync(); err != nil {
		return nil
	}

	// 持久化完成后，写记录Merge完成，单独开一个mergeFinished文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return nil
	}

	mergeFinishedRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
		Type:  0,
	}

	encRecord, _ := data.EncodeLogRecord(mergeFinishedRecord)
	if err = mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}

	if err = mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

// needMerge 判断是否需要merge
func (db *DB) needMerge() (bool, error) {
	totalSize, err := utils.DirSize(db.option.DirPath)
	if err != nil {
		return false, err
	}

	// 判断当前系统能否有足够的空间容纳merge数据量
	// 即可用空间 * 配置系数 > 有效数据占用空间
	availableSize, err := utils.AvailableDiskSize()
	if err != nil {
		return false, err
	}

	// 如果小于则不能进行合并
	if float32(availableSize)*db.option.mergeSpaceRatioThr <= float32(totalSize)-float32(db.recycleSize) {
		return false, ErrMergeCondUnreached
	}

	// 当可回收数据大于最大值
	// 或者可回收数据大于最小值，并且可回收数据 / 总数据 >= 阈值比例
	if (db.recycleSize >= db.option.mergeMaxSizeThr) ||
		(db.recycleSize >= db.option.mergeMinSizeThr && float32(db.recycleSize)/float32(totalSize) >= db.option.mergeRatioThr) {
		return true, nil
	}

	return false, nil
}

// 得到merge目录
// 比如当前文件是在/tmp/bitcask目录下
// 那么merge目录为/tmp/bitcask-merge
func (db *DB) getMergePath() string {
	// dir得到目录，Dir返回上一层目录，Clean去掉斜杠
	// base得到当前目录名称
	dir := filepath.Dir(filepath.Clean(db.option.DirPath))
	base := filepath.Base(db.option.DirPath)

	return filepath.Join(dir, base+mergeDirName)
}

// 加载merge目录
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}

	// 加载完成之后删除merge目录
	defer func() {
		if err := os.RemoveAll(mergePath); err != nil {
			panic("delete merge directory failed")
		}
	}()

	// 加载目录下的每个文件
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 判断是否存在MergeFinished文件
	var mergeFinished = false
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}

		// 不需要把flock拷贝过去了
		if entry.Name() == fileLockName {
			continue
		}

		// For example, Name would return "hello.go" not "home/gopher/hello.go".
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	if !mergeFinished {
		return nil
	}

	// 找到最大的没有merge的文件id，将已经merge的文件进行删除
	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	// 删除旧的数据文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.option.DirPath, fileId)
		if _, err := os.Stat(fileName); os.IsNotExist(err) {
			continue
		}

		// 存在的话就进行删除
		err := os.Remove(fileName)
		if err != nil {
			return err
		}
	}

	// 将merge目录下的新文件移动到db.option.DirPath中
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		desPath := filepath.Join(db.option.DirPath, fileName)

		// go使用rename进行移动文件
		if err := os.Rename(srcPath, desPath); err != nil {
			return nil
		}
	}

	return nil
}

// 加载索引文件
func (db *DB) loadIndexFromHintFile() error {
	hintFileName := filepath.Join(db.option.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	hintFile, err := data.OpenHintFile(db.option.DirPath)
	if err != nil {
		return err
	}

	// 读取文件中的索引
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		pos := data.DecodeLogRecordPos(logRecord.Value)

		// hint文件中存的是realKey, 不需要处理事务id
		db.index.Put(logRecord.Key, pos)

		offset += size
	}

	return nil
}

func (db *DB) getNonMergeFileId(mergePath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return 0, err
	}

	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}

	if string(record.Key) != mergeFinishedKey {
		return 0, err
	}

	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}

	return uint32(nonMergeFileId), nil
}
