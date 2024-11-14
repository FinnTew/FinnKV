package bitcask

type Option func(*Options)

type Options struct {
	ReadWrite   bool
	SyncOnPut   bool
	MaxFileSize int64
}

func defaultOptions() *Options {
	return &Options{
		ReadWrite:   false,
		SyncOnPut:   false,
		MaxFileSize: 2 << 20, // 2 MB
	}
}

func WithReadWrite() Option {
	return func(opts *Options) {
		opts.ReadWrite = true
	}
}

func WithSyncOnPut() Option {
	return func(opts *Options) {
		opts.SyncOnPut = true
	}
}

func WithMaxFileSize(size int64) Option {
	return func(opts *Options) {
		opts.MaxFileSize = size
	}
}
