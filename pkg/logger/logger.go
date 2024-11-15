package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"sync"
)

var (
	logger *zap.Logger
	once   sync.Once
)

// Init 初始化日志器
func Init(level zapcore.Level) {
	once.Do(func() {
		// 设置编码器配置
		encoderConfig := zapcore.EncoderConfig{
			TimeKey:        "time",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.CapitalColorLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		}

		// 设置日志级别
		atomicLevel := zap.NewAtomicLevel()
		atomicLevel.SetLevel(level)

		// 创建核心日志器
		core := zapcore.NewCore(
			zapcore.NewConsoleEncoder(encoderConfig), // 编码器
			zapcore.AddSync(os.Stdout),               // 输出目标
			atomicLevel,                              // 日志级别
		)

		// 构建 logger
		logger = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))
	})
}

// GetLogger 获取日志器实例
func GetLogger() *zap.Logger {
	if logger == nil {
		Init(zapcore.InfoLevel)
	}
	return logger
}

// Debug 调试级别日志
func Debug(msg string, fields ...zap.Field) {
	GetLogger().Debug(msg, fields...)
}

// Info 信息级别日志
func Info(msg string, fields ...zap.Field) {
	GetLogger().Info(msg, fields...)
}

// Warn 警告级别日志
func Warn(msg string, fields ...zap.Field) {
	GetLogger().Warn(msg, fields...)
}

// Error 错误级别日志
func Error(msg string, fields ...zap.Field) {
	GetLogger().Error(msg, fields...)
}

// Fatal 致命错误级别日志
func Fatal(msg string, fields ...zap.Field) {
	GetLogger().Fatal(msg, fields...)
}
