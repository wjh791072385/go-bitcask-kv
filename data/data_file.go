package data

import "go-bitcask-kv/fio"

type DataFile struct {
	FileId    uint32
	WriteOff  int64         // 文件写入到的位置
	IoManager fio.IOManager // IO管理结构
}

// OpenDataFile 打开新的日志文件
func OpenDataFile(path string, fileid uint32) (*DataFile, error) {
	return nil, nil
}

func (file *DataFile) Sync() error {
	return nil
}

func (file *DataFile) Write([]byte) error {
	return nil
}
