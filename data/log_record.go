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

// EncodeRecordHeader 对logRecord头部进行编码
func EncodeRecordHeader(logRecord *LogRecord) ([]byte, int64) {
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

// DecodeRecordHeader 对头部进行解码，返回头部长度即可
func DecodeRecordHeader(headerBuf []byte) (*logRecordHeader, int64) {
	// 长度最短为7B
	if len(headerBuf) < minLogRecordHeaderSize {
		return nil, 0
	}

	keySize, keyOffset := binary.Uvarint(headerBuf[crc32.Size+logRecordTypeSize:])
	valueSize, valueOffset := binary.Uvarint(headerBuf[crc32.Size+logRecordTypeSize+keyOffset:])

	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(headerBuf[:crc32.Size]),
		recordType: LogRecordType(headerBuf[crc32.Size]),
		keySize:    uint32(keySize),
		valueSize:  uint32(valueSize),
	}

	return header, int64(crc32.Size + logRecordTypeSize + keyOffset + valueOffset)
}

// 计算头部和数据部分的一个CRC值
func getLogRecordCRC(logRecord *LogRecord, headerBuf []byte) uint32 {
	if logRecord == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(headerBuf)
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Key)
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Value)
	return crc
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
	headerBuf, headerSize := EncodeRecordHeader(record)

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

// DecodeLogRecord Todo 对模块功能进一步拆分
func DecodeLogRecord(buf []byte) (record *LogRecord) {
	// 这里只处理了对数据区的解码，并没有包含header部分的解码
	// 因为需要根据header部分的解码，才能知道数据长度是多少
	record = &LogRecord{
		Key:   buf[0:],
		Value: buf[0:],
	}
	return record
}
