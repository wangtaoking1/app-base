// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package main

import (
	"context"

	"github.com/wangtaoking1/app-base/log"
)

func main() {
	defer log.Flush()

	logger := log.WithValues("k1", "v1")
	logger.Info("this is info msg", "k2", "v2")
	logger.Info("this is info msg", "k3", "v3")

	ctx := logger.WithContext(context.Background())
	log.FromContext(ctx).Info("this is info msg")
}
