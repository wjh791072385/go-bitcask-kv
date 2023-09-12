package fio

const DataFilePerm = 0644

// IOManager 抽象IO管理接口，方便接入不同的IO，目前项目实现使用标准IO
type IOManager interface {
	// Read 从文件的指定偏移中读取数据，数据长度为len(b)，返回实际读取的数据长度
	Read(b []byte, offset int64) (int, error)

	// Write 写入字节数组到文件中，追加写到末尾
	Write(b []byte)(int, error)

	// Sync 刷新内存中的数据到磁盘上
	Sync() error

	// Close 关闭文件
	Close() error
}