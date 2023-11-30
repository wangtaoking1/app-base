// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package retry

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/wangtaoking1/app-base/errors"
)

func TestRetryWithTimeout(t *testing.T) {
	c := 0
	_ = RetryWithTimeout(context.Background(), 10*time.Millisecond, 35*time.Millisecond, func() error {
		c++
		return errors.WithMessage(RetryableErr, "error")
	})
	assert.Equal(t, 3, c)
}
