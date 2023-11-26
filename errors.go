package bitcaskKV

import "errors"

var (
	ErrKeyIsEmpty           = errors.New("the key is empty")
	ErrIndexUpdateFailed    = errors.New("failed to update index")
	ErrKeyNotFound          = errors.New("key not found in database")
	ErrDataFileNotFound     = errors.New("datafile is not found")
	ErrDataDirNameIncorrect = errors.New("data directory name is incorrect")
	ErrExceedMaxBatchNum    = errors.New("exceed max batch num")
	ErrMergeIsRunning       = errors.New("merge is running")
	ErrDatabaseIsUsing      = errors.New("the database directory is using by another process")
	ErrMergeCondUnreached   = errors.New("the database merge condition is unreached")
)
