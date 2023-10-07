package bitcaskKV

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_Merge(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//t.Log(db.getMergePath())
}
