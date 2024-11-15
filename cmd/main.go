package main

import (
	"FinnKV/internal/bitcask"
	"FinnKV/internal/db"
	"FinnKV/pkg/logger"
	"fmt"
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
		logger.Fatal(fmt.Sprintf("Failed to open database: %v", err))
	}
	defer func() {
		if err := myDB.Close(); err != nil {
			logger.Fatal(fmt.Sprintf("Failed to close database: %v", err))
		}
	}()

	// 使用数据库的基本操作
	key := []byte("example_key")
	value := []byte("example_value")

	// 写入数据
	if err := myDB.Put(key, value); err != nil {
		logger.Fatal(fmt.Sprintf("Failed to put key: %v", err))
	}
	logger.Info(fmt.Sprintf("Successfully put key: %v", string(key)))

	// 读取数据
	readValue, err := myDB.Get(key)
	if err != nil {
		logger.Fatal(fmt.Sprintf("Failed to get key: %v", err))
	}
	logger.Info(fmt.Sprintf("Successfully get value: %v", string(readValue)))

	// 删除数据
	if err := myDB.Delete(key); err != nil {
		logger.Fatal(fmt.Sprintf("Failed to delete key: %v", err))
	}
	logger.Info(fmt.Sprintf("Successfully delete key: %v", string(key)))

	// 尝试读取被删除的数据
	//readValue, err = myDB.Get(key)
	//if err != nil {
	//	logger.Fatal(fmt.Sprintf("Failed to get key: %v", err))
	//} else {
	//	logger.Info(fmt.Sprintf("Successfully get value: %v", string(readValue)))
	//}

	// 使用事务
	txn := myDB.BeginTransaction()

	// 在事务中执行写操作
	txnKey := []byte("txn_key")
	txnValue := []byte("txn_value")
	if err := txn.Put(txnKey, txnValue); err != nil {
		logger.Fatal(fmt.Sprintf("Failed to put key: %v", err))
	}
	logger.Info(fmt.Sprintf("Successfully put key: %v", string(txnKey)))

	// 在事务中读取数据
	txnReadValue, err := txn.Get(txnKey)
	if err != nil {
		logger.Fatal(fmt.Sprintf("Failed to get key: %v", err))
	}
	logger.Info(fmt.Sprintf("Successfully get value: %v", string(txnReadValue)))

	// 提交事务
	if err := txn.Commit(); err != nil {
		logger.Fatal(fmt.Sprintf("Failed to commit: %v", err))
	}
	logger.Info(fmt.Sprintf("Successfully commit: %v", string(txnKey)))

	// 在事务提交后读取数据
	readValue, err = myDB.Get(txnKey)
	if err != nil {
		logger.Fatal(fmt.Sprintf("Failed to get key: %v", err))
	}
	logger.Info(fmt.Sprintf("Successfully get value: %v", string(readValue)))

	// 演示事务回滚
	txn2 := myDB.BeginTransaction()
	rollbackKey := []byte("rollback_key")
	rollbackValue := []byte("rollback_value")

	if err := txn2.Put(rollbackKey, rollbackValue); err != nil {
		logger.Fatal(fmt.Sprintf("Failed to put key: %v", err))
	}
	logger.Info(fmt.Sprintf("Successfully put key: %v", string(txnKey)))

	// 回滚事务
	if err := txn2.Rollback(); err != nil {
		logger.Fatal(fmt.Sprintf("Failed to rollback: %v", err))
	}
	logger.Info(fmt.Sprintf("Successfully rollback key: %v", string(txnKey)))

	// 尝试读取被回滚的数据
	readValue, err = myDB.Get(rollbackKey)

	if err != nil {
		logger.Fatal(fmt.Sprintf("Failed to get rollback key: %v", err))
	} else {
		logger.Info(fmt.Sprintf("Successfully get value: %v", string(readValue)))
	}
}
