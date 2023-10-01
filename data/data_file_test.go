package data

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destoryFile(path string, fileId uint32) {
	filename := filepath.Join(path, SegDataFileNamePrefix+fmt.Sprintf("%09d", fileId)+SegDataFileNameSuffix)
	if err := os.RemoveAll(filename); err != nil {
		panic(err)
	}
}

func TestSegOpenDataFile(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0)
	defer destoryFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	dataFile1, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	dataFile2, err := OpenDataFile(os.TempDir(), 111)
	defer destoryFile(os.TempDir(), 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)
}

func TestSegDataFile_Write(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0)
	destoryFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("hello"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte("bitcask kv engine"))
	assert.Nil(t, err)
}

func TestSegDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0)
	destoryFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	// 只有一条记录
	record1 := &LogRecord{
		Key:   []byte("bitcask"),
		Value: []byte("kvEngine"),
		Type:  LogRecordNormal,
	}

	var offset int64 = 0
	buf, size1 := EncodeLogRecord(record1)
	dataFile.Write(buf)

	record2, size2, err := dataFile.ReadLogRecord(offset)
	assert.Nil(t, err)
	assert.Equal(t, size1, size2)
	assert.Equal(t, []byte("bitcask"), record2.Key)
	assert.Equal(t, []byte("kvEngine"), record2.Value)
	assert.Equal(t, LogRecordNormal, record2.Type)
	offset += size2

	// 追加写入第二条条记录
	record3 := &LogRecord{
		Key:   []byte("hello"),
		Value: []byte("world"),
		Type:  LogRecordDeleted,
	}
	buf, size3 := EncodeLogRecord(record3)
	dataFile.Write(buf)
	record4, size4, err := dataFile.ReadLogRecord(offset)
	assert.Nil(t, err)
	assert.Equal(t, size3, size4)
	assert.Equal(t, []byte("hello"), record4.Key)
	assert.Equal(t, []byte("world"), record4.Value)
	assert.Equal(t, LogRecordDeleted, record4.Type)
}
