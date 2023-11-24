package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

type MMap struct {
	// 官方MMap包只支持读取数据
	// bitcask中也只需要使用内存映射来加快启动速度
	reader *mmap.ReaderAt
}

func NewMMapIoManager(fileName string) (*MMap, error) {
	// 使用自带的os包，不存在则创建
	_, err := os.OpenFile(fileName, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}

	reader, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}

	return &MMap{reader: reader}, nil
}

func (m *MMap) Read(b []byte, offset int64) (int, error) {
	return m.reader.ReadAt(b, offset)
}

// Write 暂时不需要
func (m *MMap) Write([]byte) (int, error) {
	panic("implement me")
}

// Sync 暂时不需要
func (m *MMap) Sync() error {
	panic("implement me")
}

func (m *MMap) Close() error {
	return m.reader.Close()
}

// Size returns the length of the underlying memory-mapped file.
func (m *MMap) Size() (int64, error) {
	return int64(m.reader.Len()), nil
}
