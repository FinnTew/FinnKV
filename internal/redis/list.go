package redis

import "errors"

type List interface {
	LPush(values ...[]byte) int
	RPush(values ...[]byte) int
	LPop() ([]byte, error)
	RPop() ([]byte, error)
	LLen() int
	LRange(start, end int) [][]byte
}

func NewList() List {
	return &list{
		data: make([][]byte, 0),
	}
}

type list struct {
	data [][]byte
}

func (l *list) LPush(values ...[]byte) int {
	l.data = append(values, l.data...)
	return len(l.data)
}

func (l *list) RPush(values ...[]byte) int {
	l.data = append(l.data, values...)
	return len(l.data)
}

func (l *list) LPop() ([]byte, error) {
	if len(l.data) == 0 {
		return nil, errors.New("list is empty")
	}
	val := l.data[0]
	l.data = l.data[1:]
	return val, nil
}

func (l *list) RPop() ([]byte, error) {
	if len(l.data) == 0 {
		return nil, errors.New("list is empty")
	}
	val := l.data[len(l.data)-1]
	l.data = l.data[:len(l.data)-1]
	return val, nil
}

func (l *list) LLen() int {
	return len(l.data)
}

func (l *list) LRange(start, end int) [][]byte {
	length := len(l.data)
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

	return l.data[start : end+1]
}
