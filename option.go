package bitcaskKV

import (
	"go-bitcask-kv/index"
	"os"
)

const (
	fileLockName = "flock"
)

// Option 存储引擎配置项
type Option struct {
	// 数据存放目录
	DirPath string

	// 单个日志文件大小
	// 和偏移保持一致，使用int64，标准io库中Write使用的是int
	DataFileSize int64

	// 用于控制每次写入数据是否持久化
	// 一般为false，都是后续批量持久化, 即no-force
	SyncWrites bool

	// 使用共享内存加载数据文件
	MMapAtStartup bool

	// 累积大于多少字节进行一次持久化
	BytesPerSync uint

	// 索引类型
	IndexType index.IndexerType

	// 持久化索引存放路径，主要针对B+Tree，暂时不做实现
	indexPath string

	// merge空间最多占用剩余空间系数
	mergeSpaceRatioThr float32

	// merge操作无效数据占总数据比例阈值
	mergeRatioThr float32

	// merge操作 最小大小阈值
	mergeMinSizeThr uint32

	// merge操作 最大大小阈值
	mergeMaxSizeThr uint32
}

// IteratorOption 指定迭代器配置项
type IteratorOption struct {
	// 指定前缀匹配
	prefix []byte

	// 反转
	reverse bool
}

// WriteBatchOption Batch配置项
type WriteBatchOption struct {
	// 一个batch最大的数据量
	maxBatchNum int

	// 提交事务时是否持久化
	SyncWriteBatch bool
}

var DefaultOption = Option{
	DirPath: os.TempDir(),

	// 默认64M
	DataFileSize: 64 * 1024 * 1024,

	// 默认不同步刷新
	SyncWrites: false,

	// 默认使用MMap加载数据文件
	MMapAtStartup: true,

	// 默认BTree索引
	IndexType: index.BtreeIndex,

	// 默认使用BTree索引，为内存索引，不需要持久化路径
	indexPath: "",

	mergeSpaceRatioThr: 0.8,

	mergeRatioThr: 0.3,

	// 最小64M开始merge
	mergeMinSizeThr: 64 * 1024 * 1024,

	// 超过256M则需要merge
	mergeMaxSizeThr: 256 * 1024 * 1024,
}

var DefaultIteratorOption = IteratorOption{
	prefix:  nil,
	reverse: false,
}

var DefaultWriteBachOption = WriteBatchOption{
	maxBatchNum:    1024,
	SyncWriteBatch: true, // 默认最好设置为一旦commit，就进行持久化
}
