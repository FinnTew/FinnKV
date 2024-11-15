package db

import (
	"sync"
)

// VersionedValue 表示一个版本的值
type VersionedValue struct {
	value     []byte
	timestamp int64
	committed bool
}

// VersionedValues 包含一个键的所有版本，以及一个局部锁
type VersionedValues struct {
	values []*VersionedValue
	lock   sync.RWMutex
}

// MVCC 实现多版本并发控制，使用 sync.Map 和局部锁优化
type MVCC struct {
	versions sync.Map // map[string]*VersionedValues
}

// NewMVCC 创建新的 MVCC 实例
func NewMVCC() *MVCC {
	return &MVCC{}
}

// Read 读取指定时间戳之前的最新已提交版本
func (mvcc *MVCC) Read(key []byte, ts int64) ([]byte, bool) {
	rawValues, ok := mvcc.versions.Load(string(key))
	if !ok {
		return nil, false
	}

	versionedValues := rawValues.(*VersionedValues)
	versionedValues.lock.RLock()
	defer versionedValues.lock.RUnlock()

	versions := versionedValues.values

	// 从最新版本开始遍历
	for i := len(versions) - 1; i >= 0; i-- {
		vv := versions[i]
		if vv.timestamp <= ts && vv.committed {
			return vv.value, true
		}
	}
	return nil, false
}

// Write 写入新的版本
func (mvcc *MVCC) Write(key, value []byte, txnID int64) {
	vv := &VersionedValue{
		value:     value,
		timestamp: txnID,
		committed: false,
	}

	rawValues, _ := mvcc.versions.LoadOrStore(string(key), &VersionedValues{})
	versionedValues := rawValues.(*VersionedValues)
	versionedValues.lock.Lock()
	defer versionedValues.lock.Unlock()

	versionedValues.values = append(versionedValues.values, vv)
}

// Commit 提交指定事务 ID 的版本
func (mvcc *MVCC) Commit(txnID int64) error {
	mvcc.versions.Range(func(key, value interface{}) bool {
		versionedValues := value.(*VersionedValues)

		versionedValues.lock.Lock()
		for _, vv := range versionedValues.values {
			if vv.timestamp == txnID && !vv.committed {
				vv.committed = true
			}
		}
		versionedValues.lock.Unlock()

		return true
	})
	return nil
}

// Abort 回滚指定事务 ID 的版本
func (mvcc *MVCC) Abort(txnID int64) {
	mvcc.versions.Range(func(key, value interface{}) bool {
		versionedValues := value.(*VersionedValues)

		versionedValues.lock.Lock()
		var newVersions []*VersionedValue
		for _, vv := range versionedValues.values {
			if vv.timestamp != txnID {
				newVersions = append(newVersions, vv)
			}
		}
		versionedValues.values = newVersions
		empty := len(newVersions) == 0
		versionedValues.lock.Unlock()

		if empty {
			mvcc.versions.Delete(key)
		}

		return true
	})
}

// Cleanup 清理早于指定时间戳的未提交版本
func (mvcc *MVCC) Cleanup(ts int64) {
	mvcc.versions.Range(func(key, value interface{}) bool {
		versionedValues := value.(*VersionedValues)

		versionedValues.lock.Lock()
		var newVersions []*VersionedValue
		for _, vv := range versionedValues.values {
			if vv.timestamp > ts || vv.committed {
				newVersions = append(newVersions, vv)
			}
		}
		versionedValues.values = newVersions
		empty := len(newVersions) == 0
		versionedValues.lock.Unlock()

		if empty {
			mvcc.versions.Delete(key)
		}

		return true
	})
}
