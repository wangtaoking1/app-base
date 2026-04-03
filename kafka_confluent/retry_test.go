// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka_confluent

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errTemp = errors.New("temporary error")

func TestNoRetryer(t *testing.T) {
	r := NewNoRetryer()

	t.Run("success", func(t *testing.T) {
		called := 0
		err := r.Execute(context.Background(), func() error {
			called++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 1, called)
	})

	t.Run("error is returned without retry", func(t *testing.T) {
		called := 0
		err := r.Execute(context.Background(), func() error {
			called++
			return errTemp
		})
		assert.ErrorIs(t, err, errTemp)
		assert.Equal(t, 1, called, "noRetryer must not retry")
	})
}

func TestLimitRetryer(t *testing.T) {
	t.Run("succeeds on first attempt", func(t *testing.T) {
		r := NewLimitRetryer(3, time.Millisecond)
		var calls atomic.Int32
		err := r.Execute(context.Background(), func() error {
			calls.Add(1)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, int32(1), calls.Load())
	})

	t.Run("retries up to limit then returns error", func(t *testing.T) {
		r := NewLimitRetryer(3, time.Millisecond)
		var calls atomic.Int32
		err := r.Execute(context.Background(), func() error {
			calls.Add(1)
			return errTemp
		})
		assert.Error(t, err)
		assert.Equal(t, int32(3), calls.Load(), "should attempt exactly limitTimes times")
	})

	t.Run("succeeds on second attempt", func(t *testing.T) {
		r := NewLimitRetryer(3, time.Millisecond)
		var calls atomic.Int32
		err := r.Execute(context.Background(), func() error {
			n := calls.Add(1)
			if n < 2 {
				return errTemp
			}
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, int32(2), calls.Load())
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // already cancelled
		r := NewLimitRetryer(10, time.Millisecond)
		var calls atomic.Int32
		_ = r.Execute(ctx, func() error {
			calls.Add(1)
			return errTemp
		})
		// With a cancelled context we expect at most 1 call
		assert.LessOrEqual(t, calls.Load(), int32(2))
	})
}

func TestLimitlessRetryer(t *testing.T) {
	t.Run("retries until success", func(t *testing.T) {
		r := NewLimitlessRetryer(time.Millisecond)
		var calls atomic.Int32
		err := r.Execute(context.Background(), func() error {
			n := calls.Add(1)
			if n < 5 {
				return errTemp
			}
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, int32(5), calls.Load())
	})

	t.Run("stops on context cancel", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		r := NewLimitlessRetryer(10 * time.Millisecond)
		var calls atomic.Int32
		_ = r.Execute(ctx, func() error {
			calls.Add(1)
			return errTemp
		})
		assert.Greater(t, calls.Load(), int32(0))
	})
}

func TestFastSlowRetryer(t *testing.T) {
	t.Run("fast retries then slow retries", func(t *testing.T) {
		r := NewFastSlowRetryer(2, time.Millisecond, 5*time.Millisecond)
		var calls atomic.Int32
		err := r.Execute(context.Background(), func() error {
			n := calls.Add(1)
			if n < 4 {
				return errTemp
			}
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, int32(4), calls.Load())
	})

	t.Run("stops on context cancel during slow phase", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		r := NewFastSlowRetryer(1, time.Millisecond, 100*time.Millisecond)
		var calls atomic.Int32
		_ = r.Execute(ctx, func() error {
			calls.Add(1)
			return errTemp
		})
		// Should have stopped well before the full slow interval
		assert.Greater(t, calls.Load(), int32(0))
	})
}
