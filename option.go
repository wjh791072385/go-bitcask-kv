package go_bitcask_kv

import "go-bitcask-kv/index"

type Option struct {
	// 数据存放目录
	DirPath string

	// 单个日志文件大小
	// 和偏移保持一致，使用int64，标准io库中Write使用的是int
	DataFileSize int64

	// 用于控制每次写入数据是否持久化
	SyncWrites bool

	// 索引类型
	IndexType index.IndexerType
}
