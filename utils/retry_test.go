// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
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

func TestRetry(t *testing.T) {
	result := 0
	err := Retry(3, 1*time.Millisecond, func() error {
		result++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, result)
}

func TestRetry_Error(t *testing.T) {
	result := 0
	err := Retry(3, 1*time.Millisecond, func() error {
		result++
		return fmt.Errorf("err")
	})
	assert.Error(t, err)
	assert.Equal(t, 3, result)
}

func TestRetry_NotRetryErr(t *testing.T) {
	result := 0
	err := Retry(3, 1*time.Millisecond, func() error {
		result++
		return errors.Wrap(NotRetryErr, "err")
	})
	assert.Error(t, err)
	assert.Equal(t, 1, result)
}
