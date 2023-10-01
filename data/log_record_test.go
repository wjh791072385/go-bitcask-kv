package data

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeRecordHeader(t *testing.T) {
	// Normal
	record1 := &LogRecord{
		Key:   []byte("bitcask"),
		Value: []byte("kvEngine"),
		Type:  LogRecordNormal,
	}

	headBuf1, size1 := EncodeRecordHeader(record1)
	assert.Equal(t, int64(7), size1)

	// 因为crc是在最终计算的，所以计算时将前4B置为0
	assert.Equal(t, []byte{0, 0, 0, 0, 1, 7, 8}, headBuf1)
}

func TestDecodeRecordHeader(t *testing.T) {
	// Normal
	headBuf1 := []byte{167, 57, 151, 56, 1, 7, 8, 0, 0, 0, 0}

	header1, size1 := DecodeRecordHeader(headBuf1)
	assert.Equal(t, uint32(949434791), header1.crc)
	assert.Equal(t, LogRecordType(1), header1.recordType)
	assert.Equal(t, uint32(7), header1.keySize)
	assert.Equal(t, uint32(8), header1.valueSize)
	assert.Equal(t, int64(7), size1)

}

func TestEncodeLogRecord(t *testing.T) {
	// Normal
	record1 := &LogRecord{
		Key:   []byte("bitcask"),
		Value: []byte("kvEngine"),
		Type:  LogRecordNormal,
	}

	buf1, size1 := EncodeLogRecord(record1)
	assert.NotNil(t, buf1)
	assert.Equal(t, int64(22), size1)
	//t.Log(buf1)

	record2 := &LogRecord{
		Key:   []byte("bitcask"),
		Value: nil,
		Type:  LogRecordNormal,
	}

	buf2, size2 := EncodeLogRecord(record2)
	assert.NotNil(t, buf2)
	assert.Equal(t, int64(14), size2)

	// Delete
	record3 := &LogRecord{
		Key:   []byte("bitcask"),
		Value: []byte("kvEngine"),
		Type:  LogRecordDeleted,
	}

	buf3, size3 := EncodeLogRecord(record3)
	assert.NotNil(t, buf3)
	assert.Equal(t, int64(22), size3)
}

// todo DecodeLogRecord功能还需要进一步拆分
func TestDecodeLogRecord(t *testing.T) {
	dataBuf := make([]byte, 3)
	DecodeLogRecord(dataBuf)
}
