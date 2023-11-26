package utils

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"go-bitcask-kv/fio"
	"os"
	"path/filepath"
	"testing"
)

func TestDirSize(t *testing.T) {
	// 创建随机目录
	path, err := os.MkdirTemp("", "bitcask-dirSize")
	defer os.RemoveAll(path)
	assert.Nil(t, err)

	// 创建10个文件，10个子目录
	// 每个文件写入10字节
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("file-%d", i)
		fileName := filepath.Join(path, name)
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, fio.DataFilePerm)
		assert.Nil(t, err)

		buf := []byte("hello Test")
		_, err = file.Write(buf)
		assert.Nil(t, err)
	}

	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("dir-%d", i)
		dirName := filepath.Join(path, name)
		err := os.Mkdir(dirName, os.ModePerm)
		assert.Nil(t, err)
	}

	sz, err := DirSize(path)
	assert.Nil(t, err)
	assert.Equal(t, int64(10*10), sz)
}

func TestAvailableDiskSize(t *testing.T) {
	_, err := AvailableDiskSize()
	assert.Nil(t, err)
	//t.Log(size / 1024 / 1024 / 1024)
}
