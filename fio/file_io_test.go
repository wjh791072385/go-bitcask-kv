package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIOManager(t *testing.T) {
	path := filepath.Join(os.TempDir(), "tes.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join(os.TempDir(), "tes.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	len, err := fio.Write([]byte("hello\n"))
	assert.Equal(t, 6, len)
	assert.Nil(t, err)

	len, err = fio.Write([]byte(""))
	assert.Equal(t, 0, len)
	assert.Nil(t, err)

	len, err = fio.Write([]byte("bitcask"))
	assert.Equal(t, 7, len)
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join(os.TempDir(), "tes.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	len, err := fio.Write([]byte("hello kv"))
	assert.Equal(t, 8, len)
	assert.Nil(t, err)

	b := make([]byte, 8)
	n, err := fio.Read(b, 0)
	assert.Equal(t, 8, n)
	assert.Equal(t, []byte("hello kv"), b)

	len, err = fio.Write([]byte("storage"))
	assert.Equal(t, 7, len)
	assert.Nil(t, err)

	b = make([]byte, 7)
	// offset = 8
	n, err = fio.Read(b, 8)
	assert.Equal(t, 7, n)
	assert.Equal(t, []byte("storage"), b)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join(os.TempDir(), "tes.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join(os.TempDir(), "tes.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}
