// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"sync"
	"time"

	"github.com/wangtaoking1/app-base/log"
	"github.com/wangtaoking1/app-base/websocket"
)

func main() {
	_ = log.InitLogger(true)
	ctx, cancel := context.WithCancel(context.Background())

	wsClient, err := websocket.NewClient("ws://127.0.0.1:6060/ws", &MyDispatcher{})
	if err != nil {
		panic(err)
	}
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		wsClient.Run(ctx)
	}()

	go func() {
		defer wg.Done()

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = wsClient.Write(ctx, &websocket.Message{
					Action: "echo",
					Data:   []byte("this is a request"),
				})
			}
		}
	}()

	time.Sleep(5 * time.Second)
	cancel()
	wg.Wait()
}

type MyDispatcher struct {
}

func (d *MyDispatcher) Dispatch(ctx context.Context, writer websocket.Writer, message *websocket.Message) {
	log.Infof("Receive message from server: %s, time: %v", string(message.Data), time.Now())
}
