package db

import (
	"FinnKV/internal/algo"
	"FinnKV/internal/bitcask"
	"errors"
	"log"
	"path/filepath"
	"sync"
	"time"
)

// DB 封装了 Bitcask、布隆过滤器、WAL 和 MVCC
type DB struct {
	bitcask *bitcask.Bitcask
	bloom   *algo.BloomFilter
	wal     *WAL
	mvcc    *MVCC
	lock    sync.RWMutex
	options *Options
}

// Options 配置项
type Options struct {
	BloomFilterSize uint
	BloomFilterFP   float64
	// 其他配置项
}

// Open 打开数据库
func Open(dir string, bitcaskOptions []bitcask.Option, dbOptions *Options) (*DB, error) {
	bc, err := bitcask.Open(dir, bitcaskOptions...)
	if err != nil {
		return nil, err
	}

	bf := algo.NewBloomFilter(dbOptions.BloomFilterSize, dbOptions.BloomFilterFP)
	wal, err := NewWAL(filepath.Join(dir, "wal"))
	if err != nil {
		return nil, err
	}

	mvcc := NewMVCC()

	db := &DB{
		bitcask: bc,
		bloom:   bf,
		wal:     wal,
		mvcc:    mvcc,
		options: dbOptions,
	}

	// 从现有的键加载布隆过滤器
	keys, err := bc.ListKeys()
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		db.bloom.Add(key)
	}

	// 恢复未提交的事务
	err = db.Recover()
	if err != nil {
		return nil, err
	}

	return db, nil
}

// Put 写入键值对
func (db *DB) Put(key, value []byte) error {
	txn := db.BeginTransaction()
	defer func(txn *Transaction) {
		err := txn.Commit()
		if err != nil {
			log.Fatal(err)
		}
	}(txn)
	return txn.Put(key, value)
}

// Get 获取键对应的值
func (db *DB) Get(key []byte) ([]byte, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	if !db.bloom.Contains(key) {
		return nil, errors.New("key not found")
	}

	ts := time.Now().UnixNano()
	if value, ok := db.mvcc.Read(key, ts); ok {
		return value, nil
	}

	return db.bitcask.Get(key)
}

// Delete 删除键
func (db *DB) Delete(key []byte) error {
	txn := db.BeginTransaction()
	defer func(txn *Transaction) {
		err := txn.Commit()
		if err != nil {
			log.Fatal(err)
		}
	}(txn)
	return txn.Delete(key)
}

// BeginTransaction 开始一个事务
func (db *DB) BeginTransaction() *Transaction {
	return &Transaction{
		db:      db,
		writes:  make(map[string][]byte),
		startTs: time.Now().UnixNano(),
	}
}

// Recover 从 WAL 中恢复未提交的事务
func (db *DB) Recover() error {
	entries, err := db.wal.ReadAll()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		switch entry.Type {
		case bitcask.EntryTypePut:
			if err := db.bitcask.Put(entry.Key, entry.Value); err != nil {
				return err
			}
			db.bloom.Add(entry.Key)
		case bitcask.EntryTypeDelete:
			if err := db.bitcask.Delete(entry.Key); err != nil {
				return err
			}
			// 从布隆过滤器中无法删除，只能在数据层处理
		}
	}

	return db.wal.Clear()
}

// Close 关闭数据库
func (db *DB) Close() error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if err := db.wal.Close(); err != nil {
		return err
	}

	return db.bitcask.Close()
}
