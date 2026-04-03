// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka_confluent

import (
	"context"
	"time"

	"github.com/wangtaoking1/app-base/utils"
)

// Func is the function type to be retried.
type Func func() error

// Retryer executes a function with retry logic.
type Retryer interface {
	Execute(ctx context.Context, f Func) error
}

type noRetryer struct{}

func (r *noRetryer) Execute(_ context.Context, f Func) error {
	return f()
}

// NewNoRetryer returns a Retryer that does not retry.
func NewNoRetryer() Retryer {
	return &noRetryer{}
}

type limitRetryer struct {
	limitTimes int
	interval   time.Duration
}

func (r *limitRetryer) Execute(ctx context.Context, f Func) error {
	return utils.LimitRetry(ctx, r.limitTimes, r.interval, f)
}

// NewLimitRetryer returns a Retryer with a fixed retry limit.
func NewLimitRetryer(limitTimes int, interval time.Duration) Retryer {
	return &limitRetryer{limitTimes, interval}
}

type limitlessRetryer struct {
	interval time.Duration
}

func (r *limitlessRetryer) Execute(ctx context.Context, f Func) error {
	return utils.LimitlessRetry(ctx, r.interval, f)
}

// NewLimitlessRetryer returns a Retryer that retries indefinitely.
func NewLimitlessRetryer(interval time.Duration) Retryer {
	return &limitlessRetryer{interval}
}

type fastSlowRetryer struct {
	fastLimit    int
	fastInterval time.Duration
	slowInterval time.Duration
}

func (r *fastSlowRetryer) Execute(ctx context.Context, f Func) error {
	return utils.FastSlowRetry(ctx, r.fastLimit, r.fastInterval, r.slowInterval, f)
}

// NewFastSlowRetryer returns a Retryer that retries quickly at first, then slowly.
func NewFastSlowRetryer(fastLimit int, fastInterval, slowInterval time.Duration) Retryer {
	return &fastSlowRetryer{
		fastLimit:    fastLimit,
		fastInterval: fastInterval,
		slowInterval: slowInterval,
	}
}
