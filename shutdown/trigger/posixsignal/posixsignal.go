// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package posixsignal

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/wangtaoking1/app-base/shutdown"
)

// Name defines shutdown manager name.
const Name = "PosixSignalTrigger"

// trigger implements the shutdown Trigger interface that is added
// to GracefulShutdown. Initialize with New.
type trigger struct {
	signals []os.Signal
}

// GetName returns name of this trigger.
func (t *trigger) GetName() string {
	return Name
}

// Start starts listening for posix signals.
func (t *trigger) Start(executor shutdown.Executor) error {
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, t.signals...)

		// Block until a signal is received.
		<-c

		// Trigger the shutdown execution.
		executor.Execute(t)
	}()

	return nil
}

// After do exits after shutdown.
func (t *trigger) After() {
	os.Exit(0)
}

// New initializes the PosixSignalTrigger.
// You can provide os.Signal-s as arguments, if none given,
// it will use SIGINT and SIGTERM default.
func New(sig ...os.Signal) shutdown.Trigger {
	if len(sig) == 0 {
		sig = []os.Signal{syscall.SIGINT, syscall.SIGTERM}
	}

	return &trigger{
		signals: sig,
	}
}
