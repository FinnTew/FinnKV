package bitcask

import (
	"encoding/binary"
	"hash/crc32"
)

// Entry 表示一个数据条目
type Entry struct {
	Key       []byte
	Value     []byte
	Timestamp int64
}

// Encode 将 Entry 编码为字节数组
func (e *Entry) Encode() []byte {
	keySize := int32(len(e.Key))
	valueSize := int32(len(e.Value))

	buf := make([]byte, 4+8+4+4+keySize+valueSize)
	// 校验和占位，稍后填充
	binary.BigEndian.PutUint32(buf[0:4], 0)
	// 时间戳
	binary.BigEndian.PutUint64(buf[4:12], uint64(e.Timestamp))
	// Key 和 Value 的大小
	binary.BigEndian.PutUint32(buf[12:16], uint32(keySize))
	binary.BigEndian.PutUint32(buf[16:20], uint32(valueSize))
	// Key 和 Value
	copy(buf[20:20+keySize], e.Key)
	copy(buf[20+keySize:], e.Value)
	// 计算校验和
	checksum := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], checksum)
	return buf
}

// DecodeEntry 从字节数组解码为 Entry
func DecodeEntry(buf []byte) (*Entry, error) {
	if len(buf) < 20 {
		return nil, ErrInvalidEntry
	}
	checksum := binary.BigEndian.Uint32(buf[0:4])
	calcChecksum := crc32.ChecksumIEEE(buf[4:])
	if checksum != calcChecksum {
		return nil, ErrInvalidChecksum
	}
	timestamp := int64(binary.BigEndian.Uint64(buf[4:12]))
	keySize := binary.BigEndian.Uint32(buf[12:16])
	valueSize := binary.BigEndian.Uint32(buf[16:20])
	if int(20+keySize+valueSize) != len(buf) {
		return nil, ErrInvalidEntry
	}
	key := buf[20 : 20+keySize]
	value := buf[20+keySize:]
	return &Entry{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
	}, nil
}

// EntryMetadata 用于内存索引
type EntryMetadata struct {
	FileID    int64
	Offset    int64
	Size      int64
	Timestamp int64
}
