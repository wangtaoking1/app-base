// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"time"

	"github.com/wangtaoking1/app-base/utils"
)

type Func func() error

type Retryer interface {
	Execute(ctx context.Context, f Func) error
}

type noRetryer struct {
}

func (r *noRetryer) Execute(_ context.Context, f Func) error {
	return f()
}

// NewNoRetryer returns a retryer that no retry.
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

// NewLimitRetryer returns a retryer with limit times.
func NewLimitRetryer(limitTimes int, interval time.Duration) Retryer {
	return &limitRetryer{limitTimes, interval}
}

type limitlessRetryer struct {
	interval time.Duration
}

func (r *limitlessRetryer) Execute(ctx context.Context, f Func) error {
	return utils.LimitlessRetry(ctx, r.interval, f)
}

// NewLimitlessRetryer returns a retryer with no limit.
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

// NewFastSlowRetryer returns a retryer with fast and slow retry.
func NewFastSlowRetryer(fastLimit int, fastInterval, slowInterval time.Duration) Retryer {
	return &fastSlowRetryer{
		fastLimit:    fastLimit,
		fastInterval: fastInterval,
		slowInterval: slowInterval,
	}
}
