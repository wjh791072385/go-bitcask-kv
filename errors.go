package go_bitcask_kv

import "errors"

var (
	ErrKeyIsEmpty           = errors.New("the key is empty")
	ErrIndexUpdateFailed    = errors.New("failed to update index")
	ErrKeyNotFound          = errors.New("key not found in database")
	ErrDataFileNotFound     = errors.New("datafile is not found")
	ErrDataDirNameIncorrect = errors.New("data directory name is incorrect")
)
