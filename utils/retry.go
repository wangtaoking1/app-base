// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package utils

import (
	"time"

	"github.com/pkg/errors"
)

// NotRetryErr is an error that should not retry.
var NotRetryErr = errors.New("not retry error")

// LimitRetry try exec a function with limit times.
func LimitRetry(retryLimit int, interval time.Duration, f func() error) error {
	var err error
	err = f()
	if err == nil {
		return nil
	}
	if errors.Is(err, NotRetryErr) {
		return err
	}
	retryLimit -= 1
	if retryLimit <= 0 {
		return err
	}

	t := time.NewTimer(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			err = f()
			if err == nil {
				return nil
			}
			if errors.Is(err, NotRetryErr) {
				return err
			}
			retryLimit -= 1
			if retryLimit <= 0 {
				return err
			}

			t.Reset(interval)
		}
	}
}

func LimitlessRetry(interval time.Duration, f func() error) error {
	var err error
	err = f()
	if err == nil {
		return nil
	}
	if errors.Is(err, NotRetryErr) {
		return err
	}

	t := time.NewTimer(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			err = f()
			if err == nil {
				return nil
			}
			if errors.Is(err, NotRetryErr) {
				return err
			}
			t.Reset(interval)
		}
	}
}

func FastSlowRetry(fastLimit int, fastInterval, slowInterval time.Duration, f func() error) error {
	var err error
	err = LimitRetry(fastLimit, fastInterval, f)
	if err == nil {
		return nil
	}
	if errors.Is(err, NotRetryErr) {
		return err
	}

	t := time.NewTimer(slowInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			err = f()
			if err == nil {
				return nil
			}
			if errors.Is(err, NotRetryErr) {
				return err
			}
			t.Reset(slowInterval)
		}
	}
}
