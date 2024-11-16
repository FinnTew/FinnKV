package redis

type Set interface {
	SAdd(members ...[]byte) int
	SRem(members ...[]byte) int
	SMembers() [][]byte
	SIsMember(member []byte) bool
}

func NewSet() Set {
	return &set{
		data: make(map[string]struct{}),
	}
}

type set struct {
	data map[string]struct{}
}

func (s *set) SAdd(members ...[]byte) int {
	added := 0
	for _, member := range members {
		m := string(member)
		if _, exists := s.data[m]; !exists {
			s.data[m] = struct{}{}
			added++
		}
	}
	return added
}

func (s *set) SRem(members ...[]byte) int {
	removed := 0
	for _, member := range members {
		m := string(member)
		if _, exists := s.data[m]; exists {
			delete(s.data, m)
			removed++
		}
	}
	return removed
}

func (s *set) SMembers() [][]byte {
	members := make([][]byte, 0, len(s.data))
	for member := range s.data {
		members = append(members, []byte(member))
	}
	return members
}

func (s *set) SIsMember(member []byte) bool {
	_, exists := s.data[string(member)]
	return exists
}
