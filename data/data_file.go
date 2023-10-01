package data

import (
	"errors"
	"fmt"
	"go-bitcask-kv/fio"
	"hash/crc32"
	"io"
	"path/filepath"
)

type DataFile struct {
	FileId    uint32
	WriteOff  int64         // 文件写入到的位置
	IoManager fio.IOManager // IO管理结构
}

const DataFileNamePrefix = "bitcask_"
const DataFileNameSuffix = ".data"

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

// OpenDataFile 打开新的日志文件
func OpenDataFile(path string, fileId uint32) (*DataFile, error) {
	filename := filepath.Join(path, DataFileNamePrefix+fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)

	// 初始化IOManager管理接口
	ioManager, err := fio.NewIOManager(filename, fio.StandardIO)
	if err != nil {
		return nil, err
	}

	datafile := &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}

	return datafile, nil
}

// ReadLogRecord 根据offset偏移读取文件
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	// 这里需要特殊处理，比如到文件末尾不足 15B，但这其实是一条完整的记录，比如删除记录可能总大小不足15B
	// 比如key只占了1字节，那么header大小为4 + 1 + 1 = 6
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	var headerBytes int64 = maxLogRecordHeaderSize

	// 如果长度超了，那么只需要读取到末尾
	// 因为每次写入都是一条完整的记录，所以只要还没到末尾，一定是有数据的
	// 因此可以读filesize - offset
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	// 首先读取Header部分
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headSize := decodeRecordHeader(headerBuf)
	// 说明读取到了文件末尾
	if header == nil {
		return nil, 0, io.EOF
	}

	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 根据keySize和ValueSize读取数据
	// 这里转为int64仅仅是因为Read调用是需要int64
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = int64(headSize) + keySize + valueSize

	logRecord := &LogRecord{Type: header.recordType}
	if keySize > 0 && valueSize > 0 {
		keyBuf, err := df.readNBytes(keySize, offset+int64(headSize))
		if err != nil {
			return nil, 0, err
		}

		valueBuf, err := df.readNBytes(valueSize, offset+int64(headSize)+keySize)
		if err != nil {
			return nil, 0, err
		}

		logRecord.Value = valueBuf
		logRecord.Key = keyBuf
	}

	// crc校验
	// 这里注意传入的是实际头部长度出去CRC部分的数据
	crc := getLogRecordCRC(headerBuf[crc32.Size:headSize], logRecord)
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, recordSize, nil
}

func (df *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := df.IoManager.Read(b, offset)
	return b, err
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

// todo 将Read Write统一
func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}

	// 更新WriteOff
	df.WriteOff += int64(n)

	return nil
}
