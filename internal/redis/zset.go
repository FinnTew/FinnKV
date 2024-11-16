package redis

import (
	"FinnKV/internal/algo"
)

type ZSet interface {
	ZAdd(members ...ZSetMember) int
	ZRem(members ...string) int
	ZScore(member string) (float64, bool)
	ZRange(start, end int) []ZSetMember
	ZLen() int
}

type ZSetMember struct {
	Member string
	Score  float64
}

func NewZSet() ZSet {
	return &zset{
		skipList: algo.NewSkipList[ZSetKey, struct{}](lessZSetKey),
		dict:     make(map[string]float64),
	}
}

type ZSetKey struct {
	Score  float64
	Member string
}

func lessZSetKey(a, b ZSetKey) bool {
	if a.Score != b.Score {
		return a.Score < b.Score
	}
	return a.Member < b.Member
}

type zset struct {
	skipList *algo.SkipList[ZSetKey, struct{}]
	dict     map[string]float64
}

func (z *zset) ZAdd(members ...ZSetMember) int {
	added := 0
	for _, member := range members {
		oldScore, exists := z.dict[member.Member]
		z.dict[member.Member] = member.Score
		if exists {
			// 更新跳表中的节点
			z.skipList.Del(ZSetKey{Score: oldScore, Member: member.Member})
		} else {
			added++
		}
		// 添加新的节点到跳表
		z.skipList.Add(ZSetKey{Score: member.Score, Member: member.Member}, struct{}{})
	}
	return added
}

func (z *zset) ZRem(members ...string) int {
	removed := 0
	for _, member := range members {
		score, exists := z.dict[member]
		if exists {
			delete(z.dict, member)
			z.skipList.Del(ZSetKey{Score: score, Member: member})
			removed++
		}
	}
	return removed
}

func (z *zset) ZScore(member string) (float64, bool) {
	score, exists := z.dict[member]
	return score, exists
}

func (z *zset) ZRange(start, end int) []ZSetMember {
	length := z.skipList.Len()
	if length == 0 {
		return nil
	}

	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if start > end || start >= length {
		return nil
	}

	var members []ZSetMember
	iterator := z.skipList.Iterator()
	index := 0
	for {
		key, _, ok := iterator()
		if !ok {
			break
		}
		if index >= start && index <= end {
			members = append(members, ZSetMember{Member: key.Member, Score: key.Score})
		}
		if index > end {
			break
		}
		index++
	}
	return members
}

func (z *zset) ZLen() int {
	return z.skipList.Len()
}
