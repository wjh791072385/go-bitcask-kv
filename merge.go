package bitcaskKV

import (
	"go-bitcask-kv/data"
	"path/filepath"
	"sort"
)

const mergeDirName = "-merge"

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

	return nil
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
