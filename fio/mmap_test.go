package fio

import (
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestMMap_Read(t *testing.T) {
	path := filepath.Join(os.TempDir(), "MMapTest.data")
	defer destroyFile(path)

	MMapIO, err := NewMMapIoManager(path)
	assert.Nil(t, err)

	// 使用共享内存读取空白文件
	buf := make([]byte, 5)
	n, err := MMapIO.Read(buf, 0)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
	err = MMapIO.Close()
	assert.Nil(t, err)

	// 重新打开MMap
	// 使用标准IO写入部分文件内容
	fileIO, err := NewFileIOManager(path)
	assert.Nil(t, err)
	_, err = fileIO.Write([]byte("hello world"))
	assert.Nil(t, err)

	err = fileIO.Close()
	assert.Nil(t, err)

	MMapIO, err = NewMMapIoManager(path)
	assert.Nil(t, err)
	n, err = MMapIO.Read(buf, 0)
	assert.Equal(t, 5, n)
	assert.Equal(t, "hello", string(buf))
}
