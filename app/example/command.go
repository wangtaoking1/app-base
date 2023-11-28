// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"

	"github.com/wangtaoking1/app-base/app"
)

var commands []app.Command

func init() {
	commands = append(commands, runCommand())
}

func runCommand() app.Command {
	return app.NewCommand("run",
		"run sub command",
		app.WithCmdDescription("This is a run sub command"),
		app.WithCmdRunFunc(runFunc),
	)
}

func runFunc(name string) error {
	fmt.Println("this is in run func")

	return nil
}
