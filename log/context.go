package log

import (
	"context"
)

type key int

const (
	logContextKey key = iota
)

func (l *zapLogger) WithContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, logContextKey, l)
}

// WithContext returns a copy of context in which the log value is set.
func WithContext(ctx context.Context) context.Context {
	return std.WithContext(ctx)
}

// FromContext returns the value of the log key on the ctx.
func FromContext(ctx context.Context) Logger {
	if ctx != nil {
		logger := ctx.Value(logContextKey)
		if logger != nil {
			return logger.(Logger)
		}
	}

	return std
}
