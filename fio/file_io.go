package fio

import "os"

// FileIO 用于封装标准文件的IO
type FileIO struct {
	fd *os.File // 私有文件操作符
}

// NewFileIOManager 初始化标准文件IO
func NewFileIOManager(fileName string) (*FileIO, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, DataFilePerm)
	if err != nil {
		return nil, err
	}

	return &FileIO{file}, nil
}

// Read 从文件的指定位置中读取数据
func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

// Write 写入字节数组到文件中
func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

// Sync 刷新内存中的数据到磁盘上
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

// Close 关闭文件
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), err
}

// ClearFile 清空文件, 测试辅助方法
func (fio *FileIO) ClearFile() {
	fio.fd.Truncate(0)
	fio.fd.Seek(0, 0)
}
