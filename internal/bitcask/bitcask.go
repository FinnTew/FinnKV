package bitcask

import (
	"FinnKV/internal/algo"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Bitcask 引擎结构
type Bitcask struct {
	sync.RWMutex
	dir     string
	options *Options
	//dataFiles map[int64]*DataFile
	dataFiles *algo.SkipList[int64, *DataFile]
	currFile  *DataFile
	//index     map[string]*EntryMetadata
	index     *algo.SkipList[string, *EntryMetadata]
	maxFileID int64
	writable  bool
}

// Open 打开或创建一个 Bitcask 实例
func Open(dir string, opts ...Option) (*Bitcask, error) {
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}

	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	bc := &Bitcask{
		dir:     dir,
		options: options,
		dataFiles: algo.NewSkipList[int64, *DataFile](func(a, b int64) bool {
			return a < b
		}),
		index: algo.NewSkipList[string, *EntryMetadata](func(a, b string) bool {
			return a < b
		}),
		writable: options.ReadWrite,
	}
	err = bc.loadDataFiles()
	if err != nil {
		return nil, err
	}
	return bc, nil
}

// 加载数据文件并重建内存索引
func (bc *Bitcask) loadDataFiles() error {
	files, err := os.ReadDir(bc.dir)
	if err != nil {
		return err
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".data") {
			continue
		}
		fileID, err := strconv.ParseInt(strings.TrimSuffix(file.Name(), ".data"), 10, 64)
		if err != nil {
			continue
		}
		df, err := NewDataFile(bc.dir, fileID, false)
		if err != nil {
			return err
		}
		//bc.dataFiles[fileID] = df
		bc.dataFiles.Add(fileID, df)
		if fileID > bc.maxFileID {
			bc.maxFileID = fileID
		}
		err = bc.buildIndex(df)
		if err != nil {
			return err
		}
	}
	if bc.options.ReadWrite {
		bc.maxFileID++
		currFile, err := NewDataFile(bc.dir, bc.maxFileID, true)
		if err != nil {
			return err
		}
		bc.currFile = currFile
		//bc.dataFiles[bc.maxFileID] = currFile
		bc.dataFiles.Add(bc.maxFileID, currFile)
	}
	return nil
}

// 从数据文件构建内存索引
func (bc *Bitcask) buildIndex(df *DataFile) error {
	var offset int64 = 0
	fileInfo, err := df.File.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()
	for offset < fileSize {
		headerBuf := make([]byte, 20)
		_, err := df.File.ReadAt(headerBuf, offset)
		if err != nil {
			return err
		}
		checksum := binary.BigEndian.Uint32(headerBuf[0:4])
		timestamp := int64(binary.BigEndian.Uint64(headerBuf[4:12]))
		keySize := binary.BigEndian.Uint32(headerBuf[12:16])
		valueSize := binary.BigEndian.Uint32(headerBuf[16:20])

		entrySize := int64(20 + keySize + valueSize)
		buf := make([]byte, entrySize)
		_, err = df.File.ReadAt(buf, offset)
		if err != nil {
			return err
		}
		calcChecksum := crc32.ChecksumIEEE(buf[4:])
		if checksum != calcChecksum {
			return ErrInvalidChecksum
		}
		key := buf[20 : 20+keySize]
		// 构建内存索引
		//bc.index[string(key)] = &EntryMetadata{
		//	FileID:    df.FileID,
		//	Offset:    offset,
		//	Size:      entrySize,
		//	Timestamp: timestamp,
		//}
		bc.index.Add(string(key), &EntryMetadata{
			FileID:    df.FileID,
			Offset:    offset,
			Size:      entrySize,
			Timestamp: timestamp,
		})
		offset += entrySize
	}
	return nil
}

// Put 插入或更新键值对
func (bc *Bitcask) Put(key, value []byte) error {
	if !bc.options.ReadWrite {
		return errors.New("bitcask is read-only")
	}
	bc.Lock()
	defer bc.Unlock()

	entry := &Entry{
		Key:       key,
		Value:     value,
		Timestamp: time.Now().Unix(),
	}

	// 检查当前文件大小，必要时创建新的数据文件
	if bc.currFile.WriteOff >= bc.options.MaxFileSize {
		if err := bc.currFile.Sync(); err != nil {
			return err
		}
		if err := bc.currFile.Close(); err != nil {
			return err
		}
		bc.maxFileID++
		var err error
		bc.currFile, err = NewDataFile(bc.dir, bc.maxFileID, true)
		if err != nil {
			return err
		}
		//bc.dataFiles[bc.maxFileID] = bc.currFile
		bc.dataFiles.Add(bc.maxFileID, bc.currFile)
	}

	offset, err := bc.currFile.Write(entry)
	if err != nil {
		return err
	}

	//bc.index[string(key)] = &EntryMetadata{
	//	FileID:    bc.currFile.FileID,
	//	Offset:    offset,
	//	Size:      int64(len(entry.Encode())),
	//	Timestamp: entry.Timestamp,
	//}
	bc.index.Add(string(key), &EntryMetadata{
		FileID:    bc.currFile.FileID,
		Offset:    offset,
		Size:      int64(len(entry.Encode())),
		Timestamp: entry.Timestamp,
	})

	if bc.options.SyncOnPut {
		return bc.currFile.Sync()
	}
	return nil
}

// Get 根据键获取值
func (bc *Bitcask) Get(key []byte) ([]byte, error) {
	bc.RLock()
	defer bc.RUnlock()

	//meta, ok := bc.index[string(key)]
	meta, ok := bc.index.Find(string(key))
	if !ok {
		return nil, errors.New("key not found")
	}
	//df := bc.dataFiles[meta.FileID]
	df, ok := bc.dataFiles.Find(meta.FileID)
	if !ok {
		return nil, errors.New("key not found")
	}
	entry, err := df.ReadAt(meta.Offset, meta.Size)
	if err != nil {
		return nil, err
	}
	return entry.Value, nil
}

// Delete 删除键
func (bc *Bitcask) Delete(key []byte) error {
	if !bc.options.ReadWrite {
		return errors.New("bitcask is read-only")
	}
	bc.Lock()
	defer bc.Unlock()

	// 写入一个墓碑（空的值）
	entry := &Entry{
		Key:       key,
		Value:     []byte{},
		Timestamp: time.Now().Unix(),
	}

	offset, err := bc.currFile.Write(entry)
	if err != nil {
		return err
	}

	//bc.index[string(key)] = &EntryMetadata{
	//	FileID:    bc.currFile.FileID,
	//	Offset:    offset,
	//	Size:      int64(len(entry.Encode())),
	//	Timestamp: entry.Timestamp,
	//}
	bc.index.Add(string(key), &EntryMetadata{
		FileID:    bc.currFile.FileID,
		Offset:    offset,
		Size:      int64(len(entry.Encode())),
		Timestamp: entry.Timestamp,
	})
	return nil
}

// ListKeys 列出所有键
func (bc *Bitcask) ListKeys() ([][]byte, error) {
	bc.RLock()
	defer bc.RUnlock()

	keys := make([][]byte, 0, bc.index.Len())
	//for k := range bc.index {
	//	keys = append(keys, []byte(k))
	//}
	iter := bc.index.Iterator()
	for {
		key, _, ok := iter()
		if !ok {
			break
		}
		keys = append(keys, []byte(key))
	}
	return keys, nil
}

// Fold 遍历所有键值对
func (bc *Bitcask) Fold(fn func(key, value []byte, acc interface{}) interface{}, acc interface{}) interface{} {
	bc.RLock()
	defer bc.RUnlock()

	//for k, meta := range bc.index {
	//	df := bc.dataFiles[meta.FileID]
	//	entry, err := df.ReadAt(meta.Offset, meta.Size)
	//	if err != nil {
	//		continue
	//	}
	//	if len(entry.Value) == 0 {
	//		// 跳过墓碑
	//		continue
	//	}
	//	acc = fn([]byte(k), entry.Value, acc)
	//}
	iter := bc.index.Iterator()
	for {
		key, meta, ok := iter()
		if !ok {
			break
		}
		//df := bc.dataFiles[meta.FileID]
		df, ok := bc.dataFiles.Find(meta.FileID)
		if !ok {
			continue
		}
		entry, err := df.ReadAt(meta.Offset, meta.Size)
		if err != nil {
			continue
		}
		if len(entry.Value) == 0 {
			// 跳过墓碑
			continue
		}
		acc = fn([]byte(key), entry.Value, acc)
	}
	return acc
}

// Merge 合并数据文件
func (bc *Bitcask) Merge() error {
	bc.Lock()
	defer bc.Unlock()

	tempFileID := bc.maxFileID + 1
	tempDataFile, err := NewDataFile(bc.dir, tempFileID, true)
	if err != nil {
		return err
	}

	//newIndex := make(map[string]*EntryMetadata)
	//for k, meta := range bc.index {
	//	df := bc.dataFiles[meta.FileID]
	//	entry, err := df.ReadAt(meta.Offset, meta.Size)
	//	if err != nil {
	//		continue
	//	}
	//	if len(entry.Value) == 0 {
	//		// 跳过墓碑
	//		continue
	//	}
	//	offset, err := tempDataFile.Write(entry)
	//	if err != nil {
	//		return err
	//	}
	//	newIndex[k] = &EntryMetadata{
	//		FileID:    tempFileID,
	//		Offset:    offset,
	//		Size:      int64(len(entry.Encode())),
	//		Timestamp: entry.Timestamp,
	//	}
	//}

	newIndex := algo.NewSkipList[string, *EntryMetadata](func(a, b string) bool {
		return a < b
	})
	iterIndex := bc.index.Iterator()
	for {
		key, meta, ok := iterIndex()
		if !ok {
			break
		}
		df, ok := bc.dataFiles.Find(meta.FileID)
		if !ok {
			continue
		}
		entry, err := df.ReadAt(meta.Offset, meta.Size)
		if err != nil {
			continue
		}
		if len(entry.Value) == 0 {
			continue
		}
		offset, err := tempDataFile.Write(entry)
		if err != nil {
			return err
		}
		newIndex.Add(key, &EntryMetadata{
			FileID:    tempFileID,
			Offset:    offset,
			Size:      int64(len(entry.Encode())),
			Timestamp: entry.Timestamp,
		})
	}

	// 替换旧的数据文件
	//for _, df := range bc.dataFiles {
	//	if err := df.Close(); err != nil {
	//		return err
	//	}
	//	if err := os.Remove(df.File.Name()); err != nil {
	//		return err
	//	}
	//}
	iterDateFiles := bc.dataFiles.Iterator()
	for {
		_, df, ok := iterDateFiles()
		if !ok {
			break
		}
		if err := df.Close(); err != nil {
			return err
		}
		if err := os.Remove(df.File.Name()); err != nil {
			return err
		}
	}

	//bc.dataFiles = map[int64]*DataFile{
	//	tempFileID: tempDataFile,
	//}
	dataFiles := algo.NewSkipList[int64, *DataFile](func(a, b int64) bool {
		return a < b
	})
	dataFiles.Add(tempFileID, tempDataFile)
	bc.dataFiles = dataFiles
	bc.index = newIndex
	bc.maxFileID = tempFileID
	bc.currFile = tempDataFile
	return nil
}

// Sync 将当前数据文件同步到磁盘
func (bc *Bitcask) Sync() error {
	bc.Lock()
	defer bc.Unlock()

	return bc.currFile.Sync()
}

// Close 关闭 Bitcask 实例
func (bc *Bitcask) Close() error {
	bc.Lock()
	defer bc.Unlock()

	//for _, df := range bc.dataFiles {
	//	err := df.Close()
	//	if err != nil {
	//		return err
	//	}
	//}
	iter := bc.dataFiles.Iterator()
	for {
		_, df, ok := iter()
		if !ok {
			break
		}
		if err := df.Close(); err != nil {
			return err
		}
	}
	return nil
}
