package main

import (
	"FinnKV/internal/bitcask"
	"FinnKV/internal/db"
	"FinnKV/pkg/logger"
	"fmt"
)

func main() {
	dir := "./data"
	bitcaskOpts := []bitcask.Option{
		bitcask.WithReadWrite(),
		bitcask.WithSyncOnPut(),
	}
	dbOpts := &db.Options{
		BloomFilterSize: 10000,
		BloomFilterFP:   0.01,
	}
	kvdb, err := db.Open(dir, bitcaskOpts, dbOpts)
	if err != nil {
		logger.Fatal(fmt.Sprintf("Failed to open database: %v", err))
	}
	defer func(kvdb *db.DB) {
		err := kvdb.Close()
		if err != nil {
			logger.Fatal(fmt.Sprintf("Failed to close database: %v", err))
		}
	}(kvdb)
	txn := kvdb.BeginTransaction()

}
