package filter

import (
	"encoding/gob"
	"errors"
	"os"

	redis "github.com/go-redis/redis/v7"
)

//BloomFilterStorger interface
type BloomFilterStorger interface {
	exist([]uint64) bool
	add([]uint64) error
	new(uint64) error
	count() uint64
	close() error
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
func (rs *RedisStroge) close() error {
	return rs.redisConn.Close()
}

type _E struct {
	C    uint64
	Data []uint8
}
type FileStroge struct {
	_E
	path string
}

func NewFileStroge(path string) BloomFilterStorger {
	return &FileStroge{path: path}
}
func (fs *FileStroge) exist(data []uint64) bool {
	// m := make([]byte, 1)
	for _, i := range data {
		x, y := i/8, i%8
		// fs.file.ReadAt(m, int64(x))
		if (uint8(fs.Data[x]) & (1 << (7 - y))) == 0 {
			return false
		}
	}
	return true
}
func (fs *FileStroge) add(data []uint64) error {
	// m := make([]byte, 1)
	for _, i := range data {
		x, y := i/8, i%8
		fs.Data[x] = fs.Data[x] | (1 << (7 - y))
		// fs.file.Seek(int64(x), 0)
		// fs.file.Read(m)
		// fs.file.Seek(int64(x), 0)
		// fs.file.Write([]byte{uint8(m[0]) | (1 << (7 - y))})
		// if (uint8(m[0]) & (1 << (7 - y))) == 0 {
		// 	return false
		// }
	}
	fs.C++
	return nil
}
func (fs *FileStroge) close() error {
	_, err := os.Stat(fs.path)
	var file *os.File
	var err1 error
	if os.IsNotExist(err) {
		file, err1 = os.Create(fs.path)
		if err1 != nil {
			return err1
		}
	} else {
		file, err1 = os.OpenFile(fs.path, os.O_WRONLY, 0600)
		if err1 != nil {
			return err1
		}
	}
	defer file.Close()
	encode := gob.NewEncoder(file)
	err1 = encode.Encode(&_E{fs.C, fs.Data})
	if err1 != nil {
		return err1
	}
	return nil
}
func (fs *FileStroge) new(total uint64) error {
	_, err := os.Stat(fs.path)
	// fs.Data = make([]uint8, total/8)
	if os.IsNotExist(err) {
		fs.Data = make([]uint8, total/8)
	} else {

		f, err1 := os.OpenFile(fs.path, os.O_CREATE, 0600)
		if err1 != nil {
			return err1
		}
		defer f.Close()

		var temp _E
		err1 = gob.NewDecoder(f).Decode(&temp)
		if err1 != nil {
			return err1
		}
		if uint64(cap(temp.Data)) != (total / 8) {
			return errors.New("Current raw file size error")
		}
		fs.Data = temp.Data
		fs.C = temp.C
	}
	return nil
}
func (fs *FileStroge) count() uint64 {
	return fs.C
}
