// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package posixsignal

import (
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/wangtaoking1/app-base/shutdown"
)

func waitSig(t *testing.T, c <-chan int) {
	select {
	case <-c:

	case <-time.After(1 * time.Second):
		assert.Fail(t, "Timeout waiting for shutdown.")
	}
}

func TestTrigger_DefaultSignals(t *testing.T) {
	tests := []struct {
		name   string
		signal syscall.Signal
	}{
		{
			name:   "SIGINT signal",
			signal: syscall.SIGINT,
		},
		{
			name:   "SIGTERM signal",
			signal: syscall.SIGTERM,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := make(chan int, 1)
			pst := New()
			_ = pst.Start(shutdown.ExecuteFunc(func(trigger shutdown.Trigger) {
				c <- 1
			}))

			time.Sleep(time.Millisecond)

			_ = syscall.Kill(syscall.Getpid(), tc.signal)
			waitSig(t, c)
		})
	}
}

func TestTrigger_CustomSignal(t *testing.T) {
	c := make(chan int, 1)
	pst := New(syscall.SIGHUP)
	_ = pst.Start(shutdown.ExecuteFunc(func(trigger shutdown.Trigger) {
		c <- 1
	}))

	time.Sleep(time.Millisecond)

	_ = syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
	waitSig(t, c)
}
