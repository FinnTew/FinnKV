package main

import (
	"fmt"
	"log"

	"FinnKV/internal/bitcask"
)

func main() {
	// 打开 Bitcask 实例，启用写模式，写入后立即同步
	bc, err := bitcask.Open("./data", bitcask.WithReadWrite(), bitcask.WithSyncOnPut())
	if err != nil {
		log.Fatal(err)
	}
	defer func(bc *bitcask.Bitcask) {
		err := bc.Close()
		if err != nil {
			return
		}
	}(bc)

	// 插入数据
	err = bc.Put([]byte("name"), []byte("Alice"))
	if err != nil {
		log.Fatal(err)
	}

	// 获取数据
	value, err := bc.Get([]byte("name"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Value of 'name':", string(value))

	// 删除数据
	err = bc.Delete([]byte("name"))
	if err != nil {
		log.Fatal(err)
	}

	// 列出所有键
	keys, err := bc.ListKeys()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("All keys:", keys)

	// 使用 Fold 遍历所有键值对
	sum := bc.Fold(func(key, value []byte, acc interface{}) interface{} {
		fmt.Printf("Key: %s, Value: %s\n", key, value)
		return acc
	}, nil)

	fmt.Println("Fold result:", sum)

	// 执行合并操作
	err = bc.Merge()
	if err != nil {
		log.Fatal(err)
	}

	// 同步数据到磁盘
	err = bc.Sync()
	if err != nil {
		log.Fatal(err)
	}
}
