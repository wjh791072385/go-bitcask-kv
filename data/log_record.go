package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType int8

const (
	LogRecordNormal LogRecordType = iota + 1
	LogRecordDeleted
)

// Log Head Format
// +------------+------------+----------------+-----------------+-----------+-------------+
// |	crc		|    type	 |    keySize	  | 	valueSize  |    key    |    value    |
// +------------+------------+----------------+-----------------+-----------+-------------+
//		4B    		  1B   		variable 5B	      variable 5B
const (
	maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5
	minLogRecordHeaderSize = 4 + 1 + 1 + 1
	logRecordTypeSize      = 1
)

// WAL日志记录的Header部分
type logRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

func encodeRecordHeader(logRecord *LogRecord) ([]byte, int64) {
	headerBuf := make([]byte, maxLogRecordHeaderSize)

	// 预留4B
	offset := crc32.Size

	// 写入type
	headerBuf[offset] = byte(logRecord.Type)
	offset += logRecordTypeSize

	// 写入keySize valueSize
	// 这里要注意当type == LogRecordDeleted时，Value = nil, len(Value) = 0
	// 但编码后的长度是为1B
	offset += binary.PutUvarint(headerBuf[offset:], uint64(len(logRecord.Key)))
	offset += binary.PutUvarint(headerBuf[offset:], uint64(len(logRecord.Value)))

	return headerBuf[:offset], int64(offset)
}

// 不需要返回error, 返回头部长度即可
func decodeRecordHeader(headerBuf []byte) (*logRecordHeader, uint32) {
	// 长度最短为7B
	if len(headerBuf) < minLogRecordHeaderSize {
		return nil, 0
	}

	header := &logRecordHeader{
		crc: binary.LittleEndian.Uint32(headerBuf[:crc32.Size]),
	}

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
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	// 对header部分编码
	headerBuf, headerSize := encodeRecordHeader(record)

	var totalSize = headerSize + int64(len(record.Key)) + int64(len(record.Value))
	buf := make([]byte, totalSize)

	// 拷贝header部分
	copy(buf[:headerSize], headerBuf)

	// 拷贝数据部分
	copy(buf[headerSize:], record.Key)
	copy(buf[headerSize+int64(len(record.Key)):], record.Value)

	// 计算crc
	crc := crc32.ChecksumIEEE(buf[crc32.Size:])
	binary.LittleEndian.PutUint32(buf[:crc32.Size], crc)

	return buf, totalSize
}

func DecodeLogRecord(buf []byte) (record *LogRecord) {
	return nil
}
