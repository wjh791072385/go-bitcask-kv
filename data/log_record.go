package data

import "encoding/binary"

type LogRecordType int8

const (
	LogRecordNormal LogRecordType = iota + 1
	LogRecordDeleted
)

// Log Head Format
// crc    |    type	   |    key_size	| 	value_size    |    key    |		value
//
//	4B    		1B   		variable 5B	    variable 5B
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// WAL日志记录的Header部分
type logRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

func encodeRecordHeader() {

}

// 不需要返回error, 返回头部长度即可
func decodeRecordHeader([]byte) (*logRecordHeader, uint32) {
	return nil, 0
}

// 计算头部和数据部分的一个CRC值
func getLogRecordCRC(head []byte, logRecord *LogRecord) uint32 {
	return 0
}

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
