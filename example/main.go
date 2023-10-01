package main

import (
	"fmt"
	go_bitcask_kv "go-bitcask-kv"
)

func main() {
	opt := go_bitcask_kv.DefaultOption

	db, err := go_bitcask_kv.Open(opt)
	if err != nil {
		panic(err)
	}

	// 插入一条k-v
	err = db.Put([]byte("hello"), []byte("world"))
	if err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("hello"))
	if err != nil {
		panic(err)
	}

	fmt.Println("val = ", string(val))

	// 再插入一条kv
	err = db.Put([]byte("engine"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}

	val, err = db.Get([]byte("engine"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(val))

	// 查找不存在的key
	val, err = db.Get([]byte("bit"))
	fmt.Println("the key \"bit\"", err)

	// 删除一个不存在的key
	err = db.Delete([]byte("ttt"))
	fmt.Println("the key \"ttt\"", err)

	// 删除一个已存在的key
	err = db.Delete([]byte("engine"))
	if err != nil {
		panic("delete key[engine] failed")
	}
	val, err = db.Get([]byte("engine"))
	fmt.Println("the key \"engine\"", err)
}
