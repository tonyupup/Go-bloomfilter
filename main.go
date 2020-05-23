package main

import (
	"fmt"

	"github.com/tonyupup/Go-bloomfilter/filter"
)



func main() {
	// conn := redis.NewClient(&redis.Options{
	// 	Addr: "127.0.0.1",
	// })
	// defer conn.Close()

	// rs := filter.NewRedisStroge(conn, "test")
	rs := filter.NewFileStroge("m.bin")
	bf, err := filter.NewBloomFilter(rs, 0.0000001, 1000000)
	defer bf.Close()
	if err != nil {
		fmt.Println(err)
	}
	bf.Add([]byte("nihao"))
	fmt.Println(bf.Exist([]byte("nihao")))
	bf.Add([]byte("nihao1"))
	fmt.Println(bf.Exist([]byte("nihao2")))

}
