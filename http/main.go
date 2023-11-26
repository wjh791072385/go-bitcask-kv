package main

import (
	"encoding/json"
	bitcask "go-bitcask-kv"
	"log"
	"net/http"
	"os"
)

var db *bitcask.DB

func init() {
	opts := bitcask.DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-http")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024

	var err error
	db, err = bitcask.Open(opts)
	if err != nil {
		panic(err)
	}
}

// curl -X POST localhost:10010/bitcask/put -d '{"hello":"world", "kv":"engine"}'
func handlePut(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not support", http.StatusMethodNotAllowed)
		return
	}

	// post请求中携带json数据
	var data map[string]string
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	for key, value := range data {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to put key-value : %v", err)
			return
		}
	}
}

// curl "localhost:10010/bitcask/get?key=kv"
func handleGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not support", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")
	value, err := db.Get([]byte(key))
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(value))
}

// curl -X DELETE "localhost:10010/bitcask/delete?key=hello"
func handleDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodDelete {
		http.Error(writer, "method not support", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")
	err := db.Delete([]byte(key))
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("success")
}

// curl "localhost:10010/bitcask/listKeys"
func handleListKeys(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not support", http.StatusMethodNotAllowed)
		return
	}

	keys := db.ListKeys()
	var result []string
	for _, key := range keys {
		result = append(result, string(key))
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(result)
}

// curl "localhost:10010/bitcask/stat"
func handleStat(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not support", http.StatusMethodNotAllowed)
		return
	}

	stat := db.Stat()
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(stat)
}

func main() {
	// 注册处理方法
	http.HandleFunc("/bitcask/put", handlePut)
	http.HandleFunc("/bitcask/get", handleGet)
	http.HandleFunc("/bitcask/delete", handleDelete)
	http.HandleFunc("/bitcask/listKeys", handleListKeys)
	http.HandleFunc("/bitcask/stat", handleStat)

	http.ListenAndServe("localhost:10010", nil)
}
