// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package log

import (
	"log"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	std = newZapLogger(NewOptions())
	mu  sync.Mutex
)

// Init initializes logger with specified options.
func Init(opts *Options) {
	mu.Lock()
	defer mu.Unlock()

	std = newZapLogger(opts)
}

// New creates logger by opts which can custmoized by command arguments.
func New(opts *Options) Logger {
	return newZapLogger(opts)
}

func newZapLogger(opts *Options) *zapLogger {
	if opts == nil {
		opts = NewOptions()
	}

	var zapLevel zapcore.Level
	if err := zapLevel.UnmarshalText([]byte(opts.Level)); err != nil {
		zapLevel = zapcore.InfoLevel
	}
	encodeLevel := zapcore.CapitalLevelEncoder
	// when output to local path, with color is forbidden
	if opts.Format == consoleFormat && opts.EnableColor {
		encodeLevel = zapcore.CapitalColorLevelEncoder
	}

	encoderConfig := zapcore.EncoderConfig{
		MessageKey:     "message",
		LevelKey:       "level",
		TimeKey:        "timestamp",
		NameKey:        "logger",
		CallerKey:      "caller",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    encodeLevel,
		EncodeTime:     timeEncoder,
		EncodeDuration: milliSecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	loggerConfig := &zap.Config{
		Level:             zap.NewAtomicLevelAt(zapLevel),
		DisableCaller:     opts.DisableCaller,
		DisableStacktrace: opts.DisableStacktrace,
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding:         opts.Format,
		EncoderConfig:    encoderConfig,
		OutputPaths:      opts.OutputPaths,
		ErrorOutputPaths: opts.ErrorOutputPaths,
	}

	var err error
	l, err := loggerConfig.Build(zap.AddStacktrace(zapcore.PanicLevel), zap.AddCallerSkip(1))
	if err != nil {
		panic(err)
	}
	logger := &zapLogger{
		zapLogger: l.Named(opts.Name),
	}
	zap.RedirectStdLog(l)

	return logger
}

// WithValues creates a child logger and adds extra key-values to it.
func WithValues(keysAndValues ...interface{}) Logger { return std.WithValues(keysAndValues...) }

// Flush flushes any buffered log entries. Applications should take care to call before exiting.
func Flush() { std.Flush() }

// V return a leveled InfoLogger.
func V(level Level) InfoLogger { return std.V(level) }

// Debug method output debug level log.
func Debug(msg string, keysAndValues ...interface{}) {
	std.zapLogger.Sugar().Debugw(msg, keysAndValues...)
}

// Debugf method output debug level log.
func Debugf(format string, v ...interface{}) {
	std.zapLogger.Sugar().Debugf(format, v...)
}

// Info method output info level log.
func Info(msg string, keysAndValues ...interface{}) {
	std.zapLogger.Sugar().Infow(msg, keysAndValues...)
}

// Infof method output info level log.
func Infof(format string, v ...interface{}) {
	std.zapLogger.Sugar().Infof(format, v...)
}

// Warn method output warning level log.
func Warn(msg string, keysAndValues ...interface{}) {
	std.zapLogger.Sugar().Warnw(msg, keysAndValues...)
}

// Warnf method output warning level log.
func Warnf(format string, v ...interface{}) {
	std.zapLogger.Sugar().Warnf(format, v...)
}

// Error method output error level log.
func Error(msg string, keysAndValues ...interface{}) {
	std.zapLogger.Sugar().Errorw(msg, keysAndValues...)
}

// Errorf method output error level log.
func Errorf(format string, v ...interface{}) {
	std.zapLogger.Sugar().Errorf(format, v...)
}

// Panic method output panic level log.
func Panic(msg string, keysAndValues ...interface{}) {
	std.zapLogger.Sugar().Panicw(msg, keysAndValues...)
}

// Panicf method output panic level log and shutdown application.
func Panicf(format string, v ...interface{}) {
	std.zapLogger.Sugar().Panicf(format, v...)
}

// Fatal method output Fatalw level log.
func Fatal(msg string, keysAndValues ...interface{}) {
	std.zapLogger.Sugar().Fatalw(msg, keysAndValues...)
}

// Fatalf method output fatal level log.
func Fatalf(format string, v ...interface{}) {
	std.zapLogger.Sugar().Fatalf(format, v...)
}

// StdLogger returns logger of standard library which writes to supplied zap
// logger at specified level.
func StdLogger(level Level) *log.Logger {
	if std == nil {
		return nil
	}
	if l, err := zap.NewStdLogAt(std.zapLogger, level); err == nil {
		return l
	}

	return nil
}
