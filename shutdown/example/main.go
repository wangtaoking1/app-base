// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package main

import (
	"time"

	"github.com/wangtaoking1/app-base/log"
	"github.com/wangtaoking1/app-base/shutdown"
	"github.com/wangtaoking1/app-base/shutdown/trigger/posixsignal"
)

func main() {
	gs := shutdown.New(posixsignal.New())
	gs.SetErrorHandler(shutdown.ErrorFunc(func(err error) {
		log.Errorf("error: %v", err)
	}))
	gs.AddCallback(shutdown.CallbackFunc(func(trigger string) error {
		log.Info("Shutdown is triggered", "trigger", trigger)

		log.Info("Do shutdown...")
		time.Sleep(5 * time.Second)
		log.Info("Shutdown finished")

		return nil
	}))

	_ = gs.Start()

	// On service
	time.Sleep(1000 * time.Second)
}
