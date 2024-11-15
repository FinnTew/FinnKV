package bitcask

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	EntryTypePut      byte = 0 // 插入操作
	EntryTypeDelete   byte = 1 // 删除操作
	EntryTypeTxnBegin byte = 2 // 事务开始
	EntryTypeTxnEnd   byte = 3 // 事务结束
)

// Entry 表示一个数据条目
type Entry struct {
	Key       []byte
	Value     []byte
	Timestamp int64
	Type      byte  // 操作类型
	TxnID     int64 // 事务 ID
}

// Encode 将 Entry 编码为字节数组
func (e *Entry) Encode() []byte {
	keySize := int32(len(e.Key))
	valueSize := int32(len(e.Value))

	// 计算总长度
	totalSize := 4 + 1 + 8 + 8 + 4 + 4 + keySize + valueSize
	buf := make([]byte, totalSize)
	offset := 0

	// 校验和占位，稍后填充
	binary.BigEndian.PutUint32(buf[offset:], 0)
	offset += 4

	// 操作类型
	buf[offset] = e.Type
	offset += 1

	// 时间戳
	binary.BigEndian.PutUint64(buf[offset:], uint64(e.Timestamp))
	offset += 8

	// 事务 ID
	binary.BigEndian.PutUint64(buf[offset:], uint64(e.TxnID))
	offset += 8

	// Key 和 Value 的大小
	binary.BigEndian.PutUint32(buf[offset:], uint32(keySize))
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:], uint32(valueSize))
	offset += 4

	// Key
	copy(buf[offset:], e.Key)
	offset += int(keySize)

	// Value
	copy(buf[offset:], e.Value)

	// 计算校验和
	checksum := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:], checksum)

	return buf
}

// DecodeEntry 从字节数组解码为 Entry
func DecodeEntry(buf []byte) (*Entry, error) {
	if len(buf) < 25 { // 4 + 1 + 8 + 8 + 4 + 4 = 29
		return nil, ErrInvalidEntry
	}
	offset := 0

	// 校验和验证
	checksum := binary.BigEndian.Uint32(buf[offset:])
	offset += 4
	calcChecksum := crc32.ChecksumIEEE(buf[4:])
	if checksum != calcChecksum {
		return nil, ErrInvalidChecksum
	}

	// 操作类型
	entryType := buf[offset]
	offset += 1

	// 时间戳
	timestamp := int64(binary.BigEndian.Uint64(buf[offset:]))
	offset += 8

	// 事务 ID
	txnID := int64(binary.BigEndian.Uint64(buf[offset:]))
	offset += 8

	// Key 和 Value 的大小
	keySize := binary.BigEndian.Uint32(buf[offset:])
	offset += 4
	valueSize := binary.BigEndian.Uint32(buf[offset:])
	offset += 4

	totalSize := offset + int(keySize) + int(valueSize)
	if totalSize != len(buf) {
		return nil, ErrInvalidEntry
	}

	// Key
	key := buf[offset : offset+int(keySize)]
	offset += int(keySize)

	// Value
	value := buf[offset:]

	return &Entry{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
		Type:      entryType,
		TxnID:     txnID,
	}, nil
}

// EntryMetadata 用于内存索引
type EntryMetadata struct {
	FileID    int64
	Offset    int64
	Size      int64
	Timestamp int64
}
