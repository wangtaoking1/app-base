// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package main

import (
	"github.com/wangtaoking1/app-base/log"
	"github.com/wangtaoking1/app-base/websocket"
)

func main() {
	_ = log.InitLogger(true)

	dispatcher := websocket.NewDispatcher()
	options := websocket.NewOptions()
	wsServer := websocket.NewServer(dispatcher, options)
	wsServer.Run()
}
