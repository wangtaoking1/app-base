// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package main

import (
	"github.com/wangtaoking1/app-base/grpc"
	"github.com/wangtaoking1/app-base/log"
)

func main() {
	opts := grpc.NewOptions()
	grpcServer := grpc.New(opts)
	if err := grpcServer.Run(); err != nil {
		log.Fatal(err.Error())
	}
}
