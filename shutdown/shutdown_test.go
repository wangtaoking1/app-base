// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package shutdown

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type fakeTrigger struct{}

var _ Trigger = (*fakeTrigger)(nil)

func (f *fakeTrigger) GetName() string {
	return "fake"
}

func (f *fakeTrigger) Start(executor Executor) error {
	executor.Execute(f)

	return nil
}

func (f *fakeTrigger) After() {
}

func TestShutdown_CallbackCalled(t *testing.T) {
	c := make(chan int, 10)
	gs := New(&fakeTrigger{})
	for i := 0; i < 10; i++ {
		gs.AddCallback(CallbackFunc(func(name string) error {
			c <- 1

			return nil
		}))
	}

	_ = gs.Start()

	assert.Equal(t, 10, len(c), "callback not be called")
}

func TestShutdown_HandleError(t *testing.T) {
	c := make(chan int, 1)
	gs := New(&fakeTrigger{})
	gs.SetErrorHandler(ErrorFunc(func(err error) {
		c <- 1
	}))
	gs.AddCallback(CallbackFunc(func(name string) error {
		return fmt.Errorf("error")
	}))

	_ = gs.Start()
	assert.Equal(t, 1, len(c), "error handler not be called")
}
