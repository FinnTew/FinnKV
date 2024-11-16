package bitcask

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type DataFile struct {
	sync.Mutex
	File     *os.File
	FileID   int64
	WriteOff int64
}

// NewDataFile 创建新的数据文件
func NewDataFile(dir string, fileID int64, writable bool) (*DataFile, error) {
	filename := filepath.Join(dir, fmt.Sprintf("%09d.data", fileID))
	var file *os.File
	var err error
	if writable {
		file, err = os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	} else {
		file, err = os.OpenFile(filename, os.O_RDONLY, 0644)
	}
	if err != nil {
		return nil, err
	}
	writeOff, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		File:     file,
		FileID:   fileID,
		WriteOff: writeOff,
	}, nil
}

// Write 写入 Entry，并返回偏移量
func (df *DataFile) Write(e *Entry) (int64, error) {
	df.Lock()
	defer df.Unlock()

	buf := e.Encode()
	offset := df.WriteOff
	n, err := df.File.WriteAt(buf, offset)
	if err != nil {
		return 0, err
	}
	df.WriteOff += int64(n)
	return offset, nil
}

// ReadAt 从指定偏移量读取指定大小的数据
func (df *DataFile) ReadAt(offset int64, size int64) (*Entry, error) {
	buf := make([]byte, size)
	_, err := df.File.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return DecodeEntry(buf)
}

func safeClose(file *os.File) error {
	if err := file.Close(); err != nil {
		var pathErr *os.PathError
		if errors.As(err, &pathErr) {
			if errors.Is(pathErr.Err, os.ErrClosed) {
				return nil
			}
		}
		return err
	}
	return nil
}

// Close 关闭数据文件
func (df *DataFile) Close() error {
	return safeClose(df.File)
}

// Sync 将数据文件同步到磁盘
func (df *DataFile) Sync() error {
	df.Lock()
	defer df.Unlock()
	return df.File.Sync()
}
