package filter

import (
	"errors"

	redis "github.com/go-redis/redis/v7"
)

//BloomFilterStorger interface
type BloomFilterStorger interface {
	exist([]uint64) bool
	add([]uint64) error
	new(uint64) error
	count() uint64
}
type RedisStroge struct {
	redisConn *redis.Client
	key       string
}

func NewRedisStroge(redConn *redis.Client, key string) BloomFilterStorger {
	return &RedisStroge{redisConn: redConn, key: key}
}
func (rs *RedisStroge) new(total uint64) error {
	c := rs.redisConn.Ping()
	if c.Val() != "PONG" {
		return c.Err()
	}
	keys := rs.redisConn.Keys(rs.key).Val()
	if len(keys) == 0 {
		rs.redisConn.Set(rs.key+"_Count", 0, -1)
		return rs.redisConn.SetRange(rs.key, int64(total/8)-1, "0").Err()
	}
	if keys[0] == rs.key && rs.redisConn.StrLen(rs.key).Val() != int64(total/8) {
		return errors.New("exist key size error")
	}
	return nil
}
func (rs *RedisStroge) exist(data []uint64) bool {
	pipe := rs.redisConn.Pipeline()
	defer pipe.Close()
	result2 := make([]*redis.IntCmd, len(data))
	for index, i := range data {
		result2[index] = pipe.GetBit(rs.key, int64(i))
	}
	if _, err := pipe.Exec(); err != nil {
		panic(err)
	}
	for _, i := range result2 {
		if i.Err() != nil {
			panic(i.Err())
		}
		if i.Val() != 1 {
			return false
		}
	}
	return true
}
func (rs *RedisStroge) add(data []uint64) error {
	pipe := rs.redisConn.Pipeline()
	defer pipe.Close()
	for _, i := range data {
		pipe.SetBit(rs.key, int64(i), 1)
	}
	pipe.Incr(rs.key + "_Count")
	_, err := pipe.Exec()
	if err != nil {
		return err
	}
	return nil

}
func (rs *RedisStroge) count() uint64 {
	out, err := rs.redisConn.Get(rs.key + "_Count").Uint64()
	if err != nil {
		panic(err)
	}
	return out
}

// type FileStroge struct {
// 	file *os.File
// 	path string
// }

// func NewFileStroge(path string) BloomFilterStorger {
// 	return &FileStroge{path: path}
// }
// func (fs *FileStroge) exist(data []uint64) bool {
// 	m := make([]byte, 1)
// 	for _, i := range data {
// 		x, y := i/8, i%8
// 		fs.file.ReadAt(m, int64(x))
// 		if (uint8(m[0]) & (1 << (7 - y))) == 0 {
// 			return false
// 		}
// 	}
// 	return true
// }
// func (fs *FileStroge) add(data []uint64) error {
// 	m := make([]byte, 1)
// 	for _, i := range data {
// 		x, y := i/8, i%8
// 		fs.file.Seek(int64(x), 0)
// 		fs.file.Read(m)
// 		fs.file.Seek(int64(x), 0)
// 		fs.file.Write([]byte{uint8(m[0]) | (1 << (7 - y))})
// 		// if (uint8(m[0]) & (1 << (7 - y))) == 0 {
// 		// 	return false
// 		// }
// 	}
// 	return nil
// }
// func (fs *FileStroge) new(total uint64) error {
// 	stat, err := os.Stat(fs.path)
// 	if os.IsNotExist(err) {
// 		file, err1 := os.Create(fs.path)
// 		if err1 != nil {
// 			return err1
// 		}
// 		var i uint64 = 1
// 		for ; i < total/8; i++ {
// 			n, err2 := file.Write([]byte{0})
// 			if n != 1 || err2 != nil {
// 				return err2
// 			}
// 		}
// 		fs.file = file
// 	} else {
// 		if stat.Size() != int64(total/8)-1 {
// 			return errors.New("File size error")
// 		}
// 	}
// 	return nil
// }
// func (fs *FileStroge) count() uint64 {
// 	return 0
// }