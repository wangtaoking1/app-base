// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package main

import (
	"github.com/spf13/pflag"

	"github.com/wangtaoking1/app-base/log"
)

func main() {
	opts := log.NewOptions()
	opts.AddFlags(pflag.CommandLine)
	_ = pflag.CommandLine.Set("log.level", "debug") // For test
	pflag.Parse()

	log.Init(opts)
	defer log.Flush()

	log.Debug("this is debug msg", "key", "val")
	log.Debugf("this is info msg, kv: %s=%s", "key", "val")

	log.V(log.DebugLevel).Info("this is debug msg", "key", "val")
}
