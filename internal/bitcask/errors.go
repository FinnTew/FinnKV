package bitcask

import "errors"

var (
	ErrInvalidChecksum = errors.New("invalid checksum")
	ErrInvalidEntry    = errors.New("invalid entry")
)
