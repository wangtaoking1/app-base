// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package utils

import (
	"time"

	"github.com/pkg/errors"
)

// NotRetryErr is an error that should not retry.
var NotRetryErr = errors.New("not retry error")

// Retry try exec a function with limit times.
func Retry(retryLimit int, interval time.Duration, f func() error) error {
	var err error
	for i := 0; i < retryLimit; i++ {
		err = f()
		if err == nil {
			return nil
		}
		if errors.Is(err, NotRetryErr) {
			return err
		}
		time.Sleep(interval)
	}
	return err
}
