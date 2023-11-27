package redisSub

import "errors"

// Del 删除对应的key，通用方法
func (rds *RedisData) Del(key []byte) error {
	return rds.db.Delete(key)
}

func (rds *RedisData) Type(key []byte) (redisDataType, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		// 0 is invalid
		return 0, err
	}
	if len(encValue) == 0 {
		return 0, errors.New("type value is invalid")
	}
	return redisDataType(encValue[0]), nil
}
