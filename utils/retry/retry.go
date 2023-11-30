// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package retry

import (
	"context"
	goerrors "errors"
	"fmt"
	"math"
	"time"
)

var (
	// RetryableErr defines the error that can be retryed.
	RetryableErr = fmt.Errorf("retry")
	// TimeoutErr defines the timeout error.
	TimeoutErr = fmt.Errorf("retry timeout")
)

// RetryWithTimeout retries until timeout with specified interval.
func RetryWithTimeout(ctx context.Context, interval time.Duration, timeout time.Duration, do func() error) error {
	if timeout == 0 {
		timeout = time.Duration(math.MaxInt64)
	}

	t := time.NewTimer(timeout)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			return TimeoutErr
		case <-time.After(interval):
			err := do()
			if err == nil {
				return nil
			}

			if !goerrors.Is(err, RetryableErr) {
				return err
			}
		}
	}
}
