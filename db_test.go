package go_bitcask_kv

import (
	"encoding/binary"
	"testing"
)

type ABC struct {
	Key   []byte
	Value []byte
}

func TestOpen(t *testing.T) {
	a := &ABC{
		Key:   make([]byte, 2),
		Value: nil,
	}

	t.Log(len(a.Key))
	t.Log(len(a.Value))

	buf := make([]byte, 10)
	offset := binary.PutVarint(buf, int64(len(a.Value)))
	t.Log(offset)
}
