// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package websocket

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gorilla/websocket"
	"github.com/wangtaoking1/app-base/log"
)

type Client struct {
	conn       *websocket.Conn
	dispatcher Dispatcher
}

// NewClient creates a new websocket Client instance.
func NewClient(url string, dispatcher Dispatcher) (*Client, error) {
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:       conn,
		dispatcher: dispatcher,
	}, nil
}

func (c *Client) Run(ctx context.Context) {
	defer func() {
		_ = c.conn.Close()
		log.Info("Websocket client closed")
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			messageType, message, err := c.conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err) {
					log.Errorf("Websocket connect unexpected close: %v", err)
					return
				}
				log.Errorf("Read message error: %v", err)
				time.Sleep(time.Second)
				continue
			}
			if messageType == websocket.PingMessage || messageType == websocket.PongMessage {
				continue
			}
			msg := &Message{}
			if err = json.Unmarshal(message, msg); err != nil {
				log.Errorf("Unmarshal error: %v", err)
				continue
			}
			c.dispatcher.Dispatch(ctx, c, msg)
		}
	}
}

func (c *Client) Write(ctx context.Context, message *Message) error {
	select {
	case <-ctx.Done():
		return nil
	default:
		text, err := json.Marshal(message)
		if err != nil {
			return err
		}
		return c.conn.WriteMessage(websocket.TextMessage, text)
	}
}
