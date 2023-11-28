// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package log

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// InfoLogger represents the ability to log non-error messages, at a particular verbosity.
type InfoLogger interface {
	// Info logs a non-error message with the given key/value pairs as context.
	Info(msg string, keysAndValues ...interface{})
	// Infof logs a non-error format message.
	Infof(format string, v ...interface{})

	// Enabled checks whether this InfoLogger is enabled.
	Enabled() bool
}

// Logger represents the ability to log messages, both errors and not.
type Logger interface {
	// InfoLogger All Loggers implement InfoLogger.  Calling InfoLogger methods directly on
	// a Logger value is equivalent to calling them on a V(0) InfoLogger.  For
	// example, logger.Info() produces the same result as logger.V(0).Info.
	InfoLogger

	Debug(msg string, keysAndValues ...interface{})
	Debugf(format string, v ...interface{})
	Warn(msg string, keysAndValues ...interface{})
	Warnf(format string, v ...interface{})
	Error(msg string, keysAndValues ...interface{})
	Errorf(format string, v ...interface{})
	Panic(msg string, keysAndValues ...interface{})
	Panicf(format string, v ...interface{})
	Fatal(msg string, keysAndValues ...interface{})
	Fatalf(format string, v ...interface{})

	// V returns an InfoLogger value for a specific verbosity level.
	V(level Level) InfoLogger

	// WithValues adds some key-value pairs of context to a logger.
	WithValues(keysAndValues ...interface{}) Logger
	// WithContext returns a copy of context in which the log value is set.
	WithContext(ctx context.Context) context.Context

	// Flush flushes any buffered log entries. Applications should take care to call before exiting.
	Flush()
}

// noopInfoLogger is a log.InfoLogger that's always disabled, and does nothing.
type noopInfoLogger struct{}

func (l *noopInfoLogger) Enabled() bool                    { return false }
func (l *noopInfoLogger) Info(_ string, _ ...interface{})  {}
func (l *noopInfoLogger) Infof(_ string, _ ...interface{}) {}

var disabledInfoLogger InfoLogger = &noopInfoLogger{}

// infoLogger is a log.InfoLogger that uses Zap to log at a particular level.
type infoLogger struct {
	level zapcore.Level
	log   *zap.Logger
}

var _ InfoLogger = (*infoLogger)(nil)

func (l *infoLogger) Enabled() bool { return true }
func (l *infoLogger) Info(msg string, keysAndValues ...interface{}) {
	if checkedEntry := l.log.Check(l.level, msg); checkedEntry != nil {
		checkedEntry.Write(handleFields(l.log, keysAndValues)...)
	}
}

func (l *infoLogger) Infof(format string, args ...interface{}) {
	if checkedEntry := l.log.Check(l.level, fmt.Sprintf(format, args...)); checkedEntry != nil {
		checkedEntry.Write()
	}
}

// zapLogger is a log.Logger that uses Zap to log.
type zapLogger struct {
	zapLogger *zap.Logger
}

var _ Logger = (*zapLogger)(nil)

// newLogger creates a new log.Logger using the given Zap Logger to log.
func newLogger(l *zap.Logger) Logger {
	return &zapLogger{
		zapLogger: l,
	}
}

func (l *zapLogger) Enabled() bool { return true }

func (l *zapLogger) V(level Level) InfoLogger {
	if l.zapLogger.Core().Enabled(level) {
		return &infoLogger{
			level: level,
			log:   l.zapLogger,
		}
	}

	return disabledInfoLogger
}

func (l *zapLogger) WithValues(keysAndValues ...interface{}) Logger {
	logger := l.zapLogger.With(handleFields(l.zapLogger, keysAndValues)...)

	return newLogger(logger)
}

func (l *zapLogger) Flush() {
	_ = l.zapLogger.Sync()
}

func (l *zapLogger) Debug(msg string, keysAndValues ...interface{}) {
	l.zapLogger.Sugar().Debugw(msg, keysAndValues...)
}

func (l *zapLogger) Debugf(format string, v ...interface{}) {
	l.zapLogger.Sugar().Debugf(format, v...)
}

func (l *zapLogger) Info(msg string, keysAndValues ...interface{}) {
	l.zapLogger.Sugar().Infow(msg, keysAndValues...)
}

func (l *zapLogger) Infof(format string, v ...interface{}) {
	l.zapLogger.Sugar().Infof(format, v...)
}

func (l *zapLogger) Warn(msg string, keysAndValues ...interface{}) {
	l.zapLogger.Sugar().Warnw(msg, keysAndValues...)
}

func (l *zapLogger) Warnf(format string, v ...interface{}) {
	l.zapLogger.Sugar().Warnf(format, v...)
}

func (l *zapLogger) Error(msg string, keysAndValues ...interface{}) {
	l.zapLogger.Sugar().Errorw(msg, keysAndValues...)
}

func (l *zapLogger) Errorf(format string, v ...interface{}) {
	l.zapLogger.Sugar().Errorf(format, v...)
}

func (l *zapLogger) Panic(msg string, keysAndValues ...interface{}) {
	l.zapLogger.Sugar().Panicw(msg, keysAndValues...)
}

func (l *zapLogger) Panicf(format string, v ...interface{}) {
	l.zapLogger.Sugar().Panicf(format, v...)
}

func (l *zapLogger) Fatal(msg string, keysAndValues ...interface{}) {
	l.zapLogger.Sugar().Fatalw(msg, keysAndValues...)
}

func (l *zapLogger) Fatalf(format string, v ...interface{}) {
	l.zapLogger.Sugar().Fatalf(format, v...)
}
