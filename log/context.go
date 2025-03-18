// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package log

import (
	"context"

	"go.uber.org/zap"
)

type key int

const (
	logContextKey key = iota
)

// With returns a new logger with specified fields.
func With(args ...interface{}) *zap.SugaredLogger {
	if defaultLogger == nil {
		return nil
	}
	return defaultLogger.Sugar().With(args...)
}

// WithContext returns a copy of context in which the log value is set.
func WithContext(ctx context.Context, args ...any) context.Context {
	logger := From(ctx)
	return context.WithValue(ctx, logContextKey, logger.With(args...))
}

// From returns the value of the log key on the ctx.
func From(ctx context.Context) *zap.SugaredLogger {
	if ctx != nil {
		logger := ctx.Value(logContextKey)
		if logger != nil {
			return logger.(*zap.SugaredLogger)
		}
	}
	return SugarLogger()
}
