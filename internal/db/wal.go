package db

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"

	"FinnKV/internal/bitcask"
)

// WAL 表示写前日志
type WAL struct {
	file  *os.File
	mutex sync.Mutex
}

// NewWAL 创建新的 WAL 实例
func NewWAL(dir string) (*WAL, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	walFilePath := filepath.Join(dir, "wal.log")

	walFile, err := os.OpenFile(walFilePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: walFile,
	}, nil
}

// Write 将 Entry 写入 WAL
func (wal *WAL) Write(entry *bitcask.Entry) error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	data := entry.Encode()
	length := uint32(len(data))
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, length)
	_, err := wal.file.Write(buf)
	if err != nil {
		return err
	}
	_, err = wal.file.Write(data)
	return err
}

// Sync 同步 WAL 到磁盘
func (wal *WAL) Sync() error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	return wal.file.Sync()
}

// ReadAll 读取所有未提交的 Entry
func (wal *WAL) ReadAll() ([]*bitcask.Entry, error) {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	if _, err := wal.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var entries []*bitcask.Entry
	var txnEntries []*bitcask.Entry
	var inTransaction bool

	for {
		lengthBuf := make([]byte, 4)
		_, err := io.ReadFull(wal.file, lengthBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		length := binary.BigEndian.Uint32(lengthBuf)
		data := make([]byte, length)
		_, err = io.ReadFull(wal.file, data)
		if err != nil {
			return nil, err
		}
		entry, err := bitcask.DecodeEntry(data)
		if err != nil {
			return nil, err
		}

		switch entry.Type {
		case bitcask.EntryTypeTxnBegin:
			inTransaction = true
			txnEntries = []*bitcask.Entry{}
		case bitcask.EntryTypeTxnEnd:
			if inTransaction {
				entries = append(entries, txnEntries...)
				inTransaction = false
			}
		default:
			if inTransaction {
				txnEntries = append(txnEntries, entry)
			} else {
				// 非事务的 Entry，直接添加
				entries = append(entries, entry)
			}
		}
	}
	return entries, nil
}

// Clear 清空 WAL 文件
func (wal *WAL) Clear() error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	if err := wal.file.Truncate(0); err != nil {
		return err
	}
	_, err := wal.file.Seek(0, io.SeekStart)
	return err
}

// Close 关闭 WAL 文件
func (wal *WAL) Close() error {
	return wal.file.Close()
}
