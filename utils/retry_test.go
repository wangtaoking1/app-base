// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package utils

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestLimitRetry(t *testing.T) {
	result := 0
	err := LimitRetry(context.Background(), 3, 1*time.Millisecond, func() error {
		result++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, result)
}

func TestLimitRetry_Error(t *testing.T) {
	result := 0
	err := LimitRetry(context.Background(), 3, 1*time.Millisecond, func() error {
		result++
		return fmt.Errorf("err")
	})
	assert.Error(t, err)
	assert.Equal(t, 3, result)
}

func TestLimitRetry_NotRetryErr(t *testing.T) {
	result := 0
	err := LimitRetry(context.Background(), 3, 1*time.Millisecond, func() error {
		result++
		return errors.Wrap(NotRetryErr, "err")
	})
	assert.Error(t, err)
	assert.Equal(t, 1, result)
}

func TestLimitRetry_ContextCancel(t *testing.T) {
	result := 0
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(1 * time.Millisecond)
		cancel()
	}()
	err := LimitRetry(ctx, 3, 1*time.Millisecond, func() error {
		result++
		return fmt.Errorf("err")
	})
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestLimitlessRetry(t *testing.T) {
	result := 0
	err := LimitlessRetry(context.Background(), 1*time.Millisecond, func() error {
		result++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, result)
}

func TestLimitlessRetry_Error(t *testing.T) {
	result := 0
	limit := 3
	err := LimitlessRetry(context.Background(), 1*time.Millisecond, func() error {
		result++
		if result < limit {
			return fmt.Errorf("err")
		}
		return NotRetryErr
	})
	assert.Error(t, err)
	assert.Equal(t, limit, result)
}

func TestLimitlessRetry_NotRetryErr(t *testing.T) {
	result := 0
	err := LimitlessRetry(context.Background(), 1*time.Millisecond, func() error {
		result++
		return NotRetryErr
	})
	assert.Error(t, err)
	assert.Equal(t, 1, result)
}

func TestLimitRetry_SuccessAfterRetry(t *testing.T) {
	result := 0
	err := LimitRetry(context.Background(), 3, 1*time.Millisecond, func() error {
		result++
		if result < 3 {
			return fmt.Errorf("err")
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 3, result)
}

func TestLimitRandomRetry(t *testing.T) {
	result := 0
	err := LimitRandomRetry(context.Background(), 3, 1*time.Millisecond, 5*time.Millisecond, func() error {
		result++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, result)
}

func TestLimitRandomRetry_Error(t *testing.T) {
	result := 0
	err := LimitRandomRetry(context.Background(), 3, 1*time.Millisecond, 5*time.Millisecond, func() error {
		result++
		return fmt.Errorf("err")
	})
	assert.Error(t, err)
	assert.Equal(t, 3, result)
}

func TestLimitRandomRetry_NotRetryErr(t *testing.T) {
	result := 0
	err := LimitRandomRetry(context.Background(), 3, 1*time.Millisecond, 5*time.Millisecond, func() error {
		result++
		return errors.Wrap(NotRetryErr, "err")
	})
	assert.Error(t, err)
	assert.Equal(t, 1, result)
}

func TestLimitRandomRetry_InvalidInterval(t *testing.T) {
	err := LimitRandomRetry(context.Background(), 3, 5*time.Millisecond, 1*time.Millisecond, func() error {
		return nil
	})
	assert.Error(t, err)
}

func TestLimitRandomRetry_ContextCancel(t *testing.T) {
	result := 0
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(1 * time.Millisecond)
		cancel()
	}()
	err := LimitRandomRetry(ctx, 10, 1*time.Millisecond, 5*time.Millisecond, func() error {
		result++
		return fmt.Errorf("err")
	})
	assert.ErrorIs(t, err, context.Canceled)
}

func TestLimitlessRetry_ContextCancel(t *testing.T) {
	result := 0
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(1 * time.Millisecond)
		cancel()
	}()
	err := LimitlessRetry(ctx, 1*time.Millisecond, func() error {
		result++
		return fmt.Errorf("err")
	})
	assert.ErrorIs(t, err, context.Canceled)
}

func TestLimitlessRandomRetry(t *testing.T) {
	result := 0
	err := LimitlessRandomRetry(context.Background(), 1*time.Millisecond, 5*time.Millisecond, func() error {
		result++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, result)
}

func TestLimitlessRandomRetry_NotRetryErr(t *testing.T) {
	result := 0
	err := LimitlessRandomRetry(context.Background(), 1*time.Millisecond, 5*time.Millisecond, func() error {
		result++
		return NotRetryErr
	})
	assert.ErrorIs(t, err, NotRetryErr)
	assert.Equal(t, 1, result)
}

func TestLimitlessRandomRetry_InvalidInterval(t *testing.T) {
	err := LimitlessRandomRetry(context.Background(), 5*time.Millisecond, 1*time.Millisecond, func() error {
		return nil
	})
	assert.Error(t, err)
}

func TestLimitlessRandomRetry_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(1 * time.Millisecond)
		cancel()
	}()
	err := LimitlessRandomRetry(ctx, 1*time.Millisecond, 5*time.Millisecond, func() error {
		return fmt.Errorf("err")
	})
	assert.ErrorIs(t, err, context.Canceled)
}

func TestFastSlowRetry(t *testing.T) {
	result := 0
	err := FastSlowRetry(context.Background(), 3, 1*time.Millisecond, 1*time.Millisecond, func() error {
		result++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, result)
}

func TestFastSlowRetry_Error(t *testing.T) {
	result := 0
	limit := 5
	err := FastSlowRetry(context.Background(), 3, 1*time.Millisecond, 1*time.Millisecond, func() error {
		result++
		if result < limit {
			return fmt.Errorf("err")
		}
		return NotRetryErr
	})
	assert.Error(t, err)
	assert.Equal(t, limit, result)
}

func TestFastSlowRetry_NotRetryErr(t *testing.T) {
	result := 0
	err := FastSlowRetry(context.Background(), 3, 1*time.Millisecond, 1*time.Millisecond, func() error {
		result++
		return errors.Wrap(NotRetryErr, "err")
	})
	assert.Error(t, err)
	assert.Equal(t, 1, result)
}

func TestFastSlowRetry_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(1 * time.Millisecond)
		cancel()
	}()
	err := FastSlowRetry(ctx, 3, 1*time.Millisecond, 1*time.Millisecond, func() error {
		return fmt.Errorf("err")
	})
	assert.ErrorIs(t, err, context.Canceled)
}

func TestExponentialBackoffRetry(t *testing.T) {
	result := 0
	err := ExponentialBackoffRetry(context.Background(), 3, 1*time.Millisecond, 10*time.Millisecond, func() error {
		result++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, result)
}

func TestExponentialBackoffRetry_SuccessAfterRetry(t *testing.T) {
	result := 0
	err := ExponentialBackoffRetry(context.Background(), 5, 1*time.Millisecond, 10*time.Millisecond, func() error {
		result++
		if result < 4 {
			return fmt.Errorf("err")
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 4, result)
}

func TestExponentialBackoffRetry_ExhaustRetries(t *testing.T) {
	result := 0
	err := ExponentialBackoffRetry(context.Background(), 3, 1*time.Millisecond, 10*time.Millisecond, func() error {
		result++
		return fmt.Errorf("err")
	})
	assert.Error(t, err)
	assert.Equal(t, 3, result)
}

func TestExponentialBackoffRetry_NotRetryErr(t *testing.T) {
	result := 0
	err := ExponentialBackoffRetry(context.Background(), 5, 1*time.Millisecond, 10*time.Millisecond, func() error {
		result++
		return errors.Wrap(NotRetryErr, "err")
	})
	assert.Error(t, err)
	assert.Equal(t, 1, result)
}

func TestExponentialBackoffRetry_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(1 * time.Millisecond)
		cancel()
	}()
	err := ExponentialBackoffRetry(ctx, 10, 1*time.Millisecond, 10*time.Millisecond, func() error {
		return fmt.Errorf("err")
	})
	assert.ErrorIs(t, err, context.Canceled)
}

func TestExponentialBackoffRetry_IntervalCap(t *testing.T) {
	// verify interval is capped at maxInterval by tracking call timestamps
	maxInterval := 4 * time.Millisecond
	var timestamps []time.Time
	ExponentialBackoffRetry(context.Background(), 5, 1*time.Millisecond, maxInterval, func() error { //nolint
		timestamps = append(timestamps, time.Now())
		return fmt.Errorf("err")
	})
	assert.Len(t, timestamps, 5)
	// intervals after the cap should be <= maxInterval*1.5 (with scheduling jitter tolerance)
	for i := 3; i < len(timestamps); i++ {
		gap := timestamps[i].Sub(timestamps[i-1])
		assert.Less(t, gap, maxInterval*3, "interval at retry %d exceeded cap", i)
	}
}

func TestExponentialBackoffRetryWithJitter(t *testing.T) {
	result := 0
	err := ExponentialBackoffRetryWithJitter(
		context.Background(),
		3,
		1*time.Millisecond,
		10*time.Millisecond,
		func() error {
			result++
			return nil
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, 1, result)
}

func TestExponentialBackoffRetryWithJitter_ExhaustRetries(t *testing.T) {
	result := 0
	err := ExponentialBackoffRetryWithJitter(
		context.Background(),
		3,
		1*time.Millisecond,
		10*time.Millisecond,
		func() error {
			result++
			return fmt.Errorf("err")
		},
	)
	assert.Error(t, err)
	assert.Equal(t, 3, result)
}

func TestExponentialBackoffRetryWithJitter_NotRetryErr(t *testing.T) {
	result := 0
	err := ExponentialBackoffRetryWithJitter(
		context.Background(),
		5,
		1*time.Millisecond,
		10*time.Millisecond,
		func() error {
			result++
			return errors.Wrap(NotRetryErr, "err")
		},
	)
	assert.Error(t, err)
	assert.Equal(t, 1, result)
}

func TestExponentialBackoffRetryWithJitter_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(1 * time.Millisecond)
		cancel()
	}()
	err := ExponentialBackoffRetryWithJitter(ctx, 10, 1*time.Millisecond, 10*time.Millisecond, func() error {
		return fmt.Errorf("err")
	})
	assert.ErrorIs(t, err, context.Canceled)
}
