package db

import (
	"FinnKV/internal/bitcask"
	"errors"
	"sync"
)

// Transaction 表示一个事务
type Transaction struct {
	db        *DB
	writes    map[string][]byte
	startTs   int64
	committed bool
	lock      sync.Mutex
}

// Put 在事务中写入键值对
func (tx *Transaction) Put(key, value []byte) error {
	tx.lock.Lock()
	defer tx.lock.Unlock()

	tx.writes[string(key)] = value
	tx.db.mvcc.Write(key, value, tx.startTs)
	return nil
}

// Get 在事务中获取键对应的值
func (tx *Transaction) Get(key []byte) ([]byte, error) {
	tx.lock.Lock()
	defer tx.lock.Unlock()

	if value, ok := tx.writes[string(key)]; ok {
		return value, nil
	}

	if value, ok := tx.db.mvcc.Read(key, tx.startTs); ok {
		return value, nil
	}

	return tx.db.Get(key)
}

// Delete 在事务中删除键
func (tx *Transaction) Delete(key []byte) error {
	tx.lock.Lock()
	defer tx.lock.Unlock()

	tx.writes[string(key)] = nil
	tx.db.mvcc.Write(key, nil, tx.startTs)
	return nil
}

// Commit 提交事务
func (tx *Transaction) Commit() error {
	tx.lock.Lock()
	defer tx.lock.Unlock()

	if tx.committed {
		return errors.New("transaction already committed")
	}

	// 写入事务开始的 Entry
	startEntry := &bitcask.Entry{
		Type:      bitcask.EntryTypeTxnBegin,
		TxnID:     tx.startTs,
		Timestamp: tx.startTs,
	}
	if err := tx.db.wal.Write(startEntry); err != nil {
		return err
	}

	// 写入所有的 Entry
	for k, v := range tx.writes {
		var entryType byte
		if v == nil {
			entryType = bitcask.EntryTypeDelete
		} else {
			entryType = bitcask.EntryTypePut
		}

		entry := &bitcask.Entry{
			Key:       []byte(k),
			Value:     v,
			Timestamp: tx.startTs,
			Type:      entryType,
			TxnID:     tx.startTs,
		}
		if err := tx.db.wal.Write(entry); err != nil {
			return err
		}
	}

	// 写入事务结束的 Entry
	endEntry := &bitcask.Entry{
		Type:      bitcask.EntryTypeTxnEnd,
		TxnID:     tx.startTs,
		Timestamp: tx.startTs,
	}
	if err := tx.db.wal.Write(endEntry); err != nil {
		return err
	}

	// 同步 WAL
	if err := tx.db.wal.Sync(); err != nil {
		return err
	}

	// 将数据写入底层存储和布隆过滤器
	for k, v := range tx.writes {
		key := []byte(k)
		if v == nil {
			if err := tx.db.bitcask.Delete(key); err != nil {
				return err
			}
			tx.db.bloom.Remove(key) // 从布隆过滤器中删除
		} else {
			if err := tx.db.bitcask.Put(key, v); err != nil {
				return err
			}
			tx.db.bloom.Add(key) // 添加到布隆过滤器
		}
	}

	// 同步底层存储
	if err := tx.db.bitcask.Sync(); err != nil {
		return err
	}

	// 清理 MVCC 中的版本
	if err := tx.db.mvcc.Commit(tx.startTs); err != nil {
		return err
	}

	// 清理过期的版本
	tx.db.mvcc.Cleanup(tx.startTs)

	tx.committed = true
	return nil
}

// Rollback 回滚事务
func (tx *Transaction) Rollback() error {
	tx.lock.Lock()
	defer tx.lock.Unlock()

	if tx.committed {
		return errors.New("transaction already committed")
	}

	// 清理未提交的版本
	tx.db.mvcc.Abort(tx.startTs)
	tx.writes = make(map[string][]byte)
	return nil
}
