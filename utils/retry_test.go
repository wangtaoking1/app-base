// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package utils

import (
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestLimitRetry(t *testing.T) {
	result := 0
	err := LimitRetry(3, 1*time.Millisecond, func() error {
		result++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, result)
}

func TestLimitRetry_Error(t *testing.T) {
	result := 0
	err := LimitRetry(3, 1*time.Millisecond, func() error {
		result++
		return fmt.Errorf("err")
	})
	assert.Error(t, err)
	assert.Equal(t, 3, result)
}

func TestLimitRetry_NotRetryErr(t *testing.T) {
	result := 0
	err := LimitRetry(3, 1*time.Millisecond, func() error {
		result++
		return errors.Wrap(NotRetryErr, "err")
	})
	assert.Error(t, err)
	assert.Equal(t, 1, result)
}

func TestLimitlessRetry(t *testing.T) {
	result := 0
	err := LimitlessRetry(1*time.Millisecond, func() error {
		result++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, result)
}

func TestLimitlessRetry_Error(t *testing.T) {
	result := 0
	limit := 3
	err := LimitlessRetry(1*time.Millisecond, func() error {
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
	err := LimitlessRetry(1*time.Millisecond, func() error {
		result++
		return NotRetryErr
	})
	assert.Error(t, err)
	assert.Equal(t, 1, result)
}

func TestFastSlowRetry_Error(t *testing.T) {
	result := 0
	limit := 5
	err := FastSlowRetry(3, 1*time.Millisecond, 1*time.Millisecond, func() error {
		result++
		if result < limit {
			return fmt.Errorf("err")
		}
		return NotRetryErr
	})
	assert.Error(t, err)
	assert.Equal(t, limit, result)
}
