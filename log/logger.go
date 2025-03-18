// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package log

import (
	"go.uber.org/zap"
)

var (
	defaultLogger *zap.Logger
	innerLogger   *zap.SugaredLogger
)

// InitLogger init the default logger.
func InitLogger(isDebug bool) error {
	var err error
	if isDebug {
		defaultLogger, err = zap.NewDevelopment(zap.AddStacktrace(zap.ErrorLevel))
	} else {
		defaultLogger, err = zap.NewProduction()
	}
	innerLogger = defaultLogger.WithOptions(zap.AddCallerSkip(1)).Sugar()
	return err
}

// Logger returns the default logger.
func Logger() *zap.Logger {
	return defaultLogger
}

// SugarLogger returns the default sugar logger.
func SugarLogger() *zap.SugaredLogger {
	if defaultLogger == nil {
		return nil
	}
	return defaultLogger.Sugar()
}

// Flush flushes logs.
func Flush() {
	if innerLogger != nil {
		_ = innerLogger.Sync()
	}
	if defaultLogger != nil {
		_ = defaultLogger.Sync()
	}
}

// Debug method output debug level log.
func Debug(args ...interface{}) {
	if innerLogger == nil {
		return
	}
	innerLogger.Debug(args...)
}

// Debugw method output debug level log.
func Debugw(msg string, keysAndValues ...interface{}) {
	if innerLogger == nil {
		return
	}
	innerLogger.Debugw(msg, keysAndValues...)
}

// Debugf method output debug level log.
func Debugf(format string, args ...interface{}) {
	if innerLogger == nil {
		return
	}
	innerLogger.Debugf(format, args...)
}

// Info method output info level log.
func Info(args ...interface{}) {
	if innerLogger == nil {
		return
	}
	innerLogger.Info(args...)
}

// Infow method output info level log.
func Infow(msg string, keysAndValues ...interface{}) {
	if innerLogger == nil {
		return
	}
	innerLogger.Infow(msg, keysAndValues...)
}

// Infof method output info level log.
func Infof(format string, args ...interface{}) {
	if innerLogger == nil {
		return
	}
	innerLogger.Infof(format, args...)
}

// Warn method output warning level log.
func Warn(args ...interface{}) {
	if innerLogger == nil {
		return
	}
	innerLogger.Warn(args...)
}

// Warnw method output warning level log.
func Warnw(msg string, keysAndValues ...interface{}) {
	if innerLogger == nil {
		return
	}
	innerLogger.Warnw(msg, keysAndValues...)
}

// Warnf method output warning level log.
func Warnf(format string, args ...interface{}) {
	if innerLogger == nil {
		return
	}
	innerLogger.Warnf(format, args...)
}

// Error method output error level log.
func Error(args ...interface{}) {
	if innerLogger == nil {
		return
	}
	innerLogger.Error(args...)
}

// Errorw method output error level log.
func Errorw(msg string, keysAndValues ...interface{}) {
	if innerLogger == nil {
		return
	}
	innerLogger.Errorw(msg, keysAndValues...)
}

// Errorf method output error level log.
func Errorf(format string, args ...interface{}) {
	if innerLogger == nil {
		return
	}
	innerLogger.Errorf(format, args...)
}

// Panic method output panic level log.
func Panic(args ...interface{}) {
	if innerLogger == nil {
		return
	}
	innerLogger.Panic(args...)
}

// Panicw method output panic level log.
func Panicw(msg string, keysAndValues ...interface{}) {
	if innerLogger == nil {
		return
	}
	innerLogger.Panicw(msg, keysAndValues...)
}

// Panicf method output panic level log and shutdown application.
func Panicf(format string, args ...interface{}) {
	if innerLogger == nil {
		return
	}
	innerLogger.Panicf(format, args...)
}

// Fatal method output fatal level log.
func Fatal(args ...interface{}) {
	if innerLogger == nil {
		return
	}
	innerLogger.Fatal(args...)
}

// Fatalw method output fatal level log.
func Fatalw(msg string, keysAndValues ...interface{}) {
	if innerLogger == nil {
		return
	}
	innerLogger.Fatalw(msg, keysAndValues...)
}

// Fatalf method output fatal level log.
func Fatalf(format string, args ...interface{}) {
	if innerLogger == nil {
		return
	}
	innerLogger.Fatalf(format, args...)
}
