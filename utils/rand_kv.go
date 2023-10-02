package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	randStr      = rand.New(rand.NewSource(time.Now().UnixNano()))
	letterString = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	//numLetterString = "0123456789"
)

// GetTestKey 根据序号生成key
func GetTestKey(number int) []byte {
	return []byte(fmt.Sprintf("bitcask-key-%09d", number))
}

// RandomValue 生成长度为n的随机字符串
func RandomValue(n int) []byte {
	buf := make([]byte, n)

	for i, _ := range buf {
		buf[i] = letterString[randStr.Intn(len(letterString))]
	}

	return []byte("bitcask-value" + string(buf))
}
