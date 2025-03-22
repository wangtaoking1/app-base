// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package websocket

import "context"

type Message struct {
	Action string `json:"action"`
	Data   []byte `json:"data"`
}

type Writer interface {
	Write(ctx context.Context, message *Message) error
}

type Dispatcher interface {
	Dispatch(ctx context.Context, writer Writer, message *Message)
}
