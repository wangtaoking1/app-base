// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"time"

	"github.com/wangtaoking1/app-base/log"
	"github.com/wangtaoking1/app-base/websocket"
)

func main() {
	_ = log.InitLogger(true)

	wsServer := websocket.NewServer(&MyDispatcher{}, websocket.NewOptions())
	wsServer.Run(context.Background())
}

type MyDispatcher struct {
}

func (d *MyDispatcher) Dispatch(ctx context.Context, writer websocket.Writer, message *websocket.Message) {
	log.Infof("Receive message from client: %s, time: %v", string(message.Data), time.Now())
	_ = writer.Write(ctx, &websocket.Message{
		Action: "echo",
		Data:   []byte("this is a response"),
	})
}
