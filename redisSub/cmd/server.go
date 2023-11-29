package main

import (
	"fmt"
	"github.com/tidwall/redcon"
	bitcaskKV "go-bitcask-kv"
	"go-bitcask-kv/redisSub"
	"sync"
)

const addr = "127.0.0.1:10010"

type BitcaskServer struct {
	// 一个redis Server包括多个DB实例，使用map来维护
	dbs map[int]*redisSub.RedisData

	// 负责消息的编解码
	server *redcon.Server

	mu sync.RWMutex
}

func main() {
	redisDB, err := redisSub.NewRedisData(bitcaskKV.DefaultOption)
	if err != nil {
		panic(err)
	}

	bitcaskServer := &BitcaskServer{
		dbs:    make(map[int]*redisSub.RedisData),
		server: nil,
		mu:     sync.RWMutex{},
	}
	// 默认打开的数据库为0号数据库
	bitcaskServer.dbs[0] = redisDB

	// 初始化server
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, nil)

	// 开启监听
	fmt.Println("bitcask server is running")
	_ = bitcaskServer.server.ListenAndServe()
}

// 接收连接请求，初始化新的client
func (bs *BitcaskServer) accept(conn redcon.Conn) bool {
	cli := new(BitcaskClient)
	bs.mu.Lock()
	defer bs.mu.Unlock()

	cli.server = bs

	// 默认为0号数据库
	cli.db = bs.dbs[0]

	// 通过context传递出去
	conn.SetContext(cli)

	return true
}

func (bs *BitcaskServer) close(conn redcon.Conn, err error) {
	// 关闭所有db实例
	for _, db := range bs.dbs {
		_ = db.Close()
	}
	_ = bs.server.Close()
}
