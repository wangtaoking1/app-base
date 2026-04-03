// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package utils

import (
	"context"
	"math/rand"
	"time"

	"github.com/pkg/errors"
)

// NotRetryErr is an error that should not retry.
var NotRetryErr = errors.New("not retry error")

// LimitRetry try exec a function with limit times.
func LimitRetry(ctx context.Context, retryLimit int, interval time.Duration, f func() error) error {
	var err error
	err = f()
	if err == nil {
		return nil
	}
	if errors.Is(err, NotRetryErr) || errors.Is(err, context.Canceled) {
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
		case <-ctx.Done():
			return ctx.Err()
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

// LimitRandomRetry try exec a function with limit times and random interval in [minInterval, maxInterval).
func LimitRandomRetry(
	ctx context.Context,
	retryLimit int,
	minInterval, maxInterval time.Duration,
	f func() error,
) error {
	if maxInterval <= minInterval {
		return errors.New("invalid interval")
	}
	interval := minInterval + time.Duration(rand.Int63n(int64(maxInterval-minInterval)))
	return LimitRetry(ctx, retryLimit, interval, f)
}

// LimitlessRetry try exec a function with limitless times.
func LimitlessRetry(ctx context.Context, interval time.Duration, f func() error) error {
	var err error
	err = f()
	if err == nil {
		return nil
	}
	if errors.Is(err, NotRetryErr) || errors.Is(err, context.Canceled) {
		return err
	}

	t := time.NewTimer(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
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

// LimitlessRandomRetry try exec a function with limitless times and random interval in [minInterval, maxInterval).
func LimitlessRandomRetry(ctx context.Context, minInterval, maxInterval time.Duration, f func() error) error {
	if maxInterval <= minInterval {
		return errors.New("invalid interval")
	}
	interval := minInterval + time.Duration(rand.Int63n(int64(maxInterval-minInterval)))
	return LimitlessRetry(ctx, interval, f)
}

// ExponentialBackoffRetry try exec a function with limit times and exponential backoff interval.
// The interval doubles each retry, capped at maxInterval.
func ExponentialBackoffRetry(
	ctx context.Context,
	retryLimit int,
	initialInterval, maxInterval time.Duration,
	f func() error,
) error {
	var err error
	err = f()
	if err == nil {
		return nil
	}
	if errors.Is(err, NotRetryErr) || errors.Is(err, context.Canceled) {
		return err
	}
	retryLimit -= 1
	if retryLimit <= 0 {
		return err
	}

	interval := initialInterval
	t := time.NewTimer(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
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
			interval = min(interval*2, maxInterval)
			t.Reset(interval)
		}
	}
}

// ExponentialBackoffRetryWithJitter try exec a function with limit times, exponential backoff interval and full jitter.
// The actual interval is a random value in [0, current exponential interval).
func ExponentialBackoffRetryWithJitter(
	ctx context.Context,
	retryLimit int,
	initialInterval, maxInterval time.Duration,
	f func() error,
) error {
	var err error
	err = f()
	if err == nil {
		return nil
	}
	if errors.Is(err, NotRetryErr) || errors.Is(err, context.Canceled) {
		return err
	}
	retryLimit -= 1
	if retryLimit <= 0 {
		return err
	}

	interval := initialInterval
	t := time.NewTimer(time.Duration(rand.Int63n(int64(interval) + 1)))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
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
			interval = min(interval*2, maxInterval)
			t.Reset(time.Duration(rand.Int63n(int64(interval) + 1)))
		}
	}
}

// FastSlowRetry try exec a function with fastLimit times with fastInterval,
// if not success, try with slowInterval.
func FastSlowRetry(ctx context.Context, fastLimit int, fastInterval, slowInterval time.Duration, f func() error) error {
	var err error
	err = LimitRetry(ctx, fastLimit, fastInterval, f)
	if err == nil {
		return nil
	}
	if errors.Is(err, NotRetryErr) || errors.Is(err, context.Canceled) {
		return err
	}

	t := time.NewTimer(slowInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
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
