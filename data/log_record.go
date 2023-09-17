package data

type LogRecordType int8

const (
	LogRecordNormal LogRecordType = iota + 1
	LogRecordDeleted
)

// LogRecord WAL日志记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos 数据内存索引
type LogRecordPos struct {
	Fid    uint32 //	文件id
	Offset int64  // 文件偏移，和标准库中Write的类型保持一致int64
}

// EncodeLogRecord 对logRecord编码
// todo 编解码后续统一补充
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	return []byte(""), 0
}
