package main

import (
	"FinnKV/internal/bitcask"
	"FinnKV/internal/db"
	"fmt"
	"log"
)

func main() {
	// 配置数据库选项
	dbOptions := &db.Options{
		BloomFilterSize: 100000, // 根据预期的键数量设置布隆过滤器大小
		BloomFilterFP:   0.01,   // 设置布隆过滤器的误判率
	}

	// 配置 Bitcask 选项
	bitcaskOptions := []bitcask.Option{
		bitcask.WithReadWrite(), // 设置为读写模式
		bitcask.WithSyncOnPut(), // 每次写入后立即同步到磁盘
	}

	// 打开数据库
	databaseDir := "./data"
	myDB, err := db.Open(databaseDir, bitcaskOptions, dbOptions)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		if err := myDB.Close(); err != nil {
			log.Fatalf("Failed to close database: %v", err)
		}
	}()

	// 使用数据库的基本操作
	key := []byte("example_key")
	value := []byte("example_value")

	// 写入数据
	if err := myDB.Put(key, value); err != nil {
		log.Fatalf("Failed to put data: %v", err)
	}
	fmt.Printf("Put key: %s, value: %s\n", key, value)

	// 读取数据
	readValue, err := myDB.Get(key)
	if err != nil {
		log.Fatalf("Failed to get data: %v", err)
	}
	fmt.Printf("Get key: %s, value: %s\n", key, readValue)

	// 删除数据
	if err := myDB.Delete(key); err != nil {
		log.Fatalf("Failed to delete data: %v", err)
	}
	fmt.Printf("Deleted key: %s\n", key)

	// 尝试读取被删除的数据
	readValue, err = myDB.Get(key)
	if err != nil {
		fmt.Printf("Key %s has been deleted.\n", key)
	} else {
		fmt.Printf("Get key: %s, value: %s\n", key, readValue)
	}

	// 使用事务
	txn := myDB.BeginTransaction()

	// 在事务中执行写操作
	txnKey := []byte("txn_key")
	txnValue := []byte("txn_value")
	if err := txn.Put(txnKey, txnValue); err != nil {
		log.Fatalf("Failed to put data in transaction: %v", err)
	}
	fmt.Printf("Transaction Put key: %s, value: %s\n", txnKey, txnValue)

	// 在事务中读取数据
	txnReadValue, err := txn.Get(txnKey)
	if err != nil {
		log.Fatalf("Failed to get data in transaction: %v", err)
	}
	fmt.Printf("Transaction Get key: %s, value: %s\n", txnKey, txnReadValue)

	// 提交事务
	if err := txn.Commit(); err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}
	fmt.Println("Transaction committed.")

	// 在事务提交后读取数据
	readValue, err = myDB.Get(txnKey)
	if err != nil {
		log.Fatalf("Failed to get data after transaction: %v", err)
	}
	fmt.Printf("After Transaction Get key: %s, value: %s\n", txnKey, readValue)

	// 演示事务回滚
	txn2 := myDB.BeginTransaction()
	rollbackKey := []byte("rollback_key")
	rollbackValue := []byte("rollback_value")

	if err := txn2.Put(rollbackKey, rollbackValue); err != nil {
		log.Fatalf("Failed to put data in transaction: %v", err)
	}
	fmt.Printf("Transaction Put key: %s, value: %s\n", rollbackKey, rollbackValue)

	// 回滚事务
	if err := txn2.Rollback(); err != nil {
		log.Fatalf("Failed to rollback transaction: %v", err)
	}
	fmt.Println("Transaction rolled back.")

	// 尝试读取被回滚的数据
	readValue, err = myDB.Get(rollbackKey)

	if err != nil {
		fmt.Printf("Key %s does not exist after rollback.\n", rollbackKey)
	} else {
		fmt.Printf("Get key: %s, value: %s\n", rollbackKey, readValue)
	}
}
