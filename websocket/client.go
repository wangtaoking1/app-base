// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package websocket

import (
	"context"
	"sync"
	"time"

	"encoding/json"
	"github.com/gorilla/websocket"
	"github.com/wangtaoking1/app-base/log"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 30 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = 10 * time.Second

	// Maximum message size allowed from peer.
	maxMessageSize = 0

	outBufferSize = 100
)

type Message struct {
	Action string
	Body   []byte
}

type Client struct {
	dispatcher *Dispatcher
	conn       *websocket.Conn
	outChan    chan *Message
}

func newClient(dispatcher *Dispatcher, conn *websocket.Conn) *Client {
	client := &Client{
		dispatcher: dispatcher,
		conn:       conn,
		outChan:    make(chan *Message, outBufferSize),
	}
	return client
}

func (client *Client) run(ctx context.Context) {
	log.Info("client start")
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		client.readPump(ctx)
		wg.Done()
	}()
	go func() {
		client.writePump(ctx)
		wg.Done()
	}()
	wg.Wait()
}

func (client *Client) readPump(ctx context.Context) {
	defer func() {
		if err := recover(); err != nil {
			return
		}
		log.Info("client read pump closed")
		client.conn.Close()
	}()
	client.conn.SetReadLimit(maxMessageSize)
	_ = client.conn.SetReadDeadline(time.Now().Add(pongWait))
	client.conn.SetPongHandler(func(string) error {
		return client.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		select {
		case <-ctx.Done():
			return
		default:
			messageType, message, err := client.conn.ReadMessage()
			if err != nil {
				return
			}
			if messageType == websocket.PongMessage ||
				messageType == websocket.PingMessage {
				continue
			}
			msg := &Message{}
			_ = json.Unmarshal(message, msg)
			if len(message) > 0 {
				client.handleAction(msg.Action, msg.Body)
			}
		}
	}
}

func (client *Client) handleAction(action string, body []byte) {
	// TODO: send the input to dispatcher, and then write the output after handled
	return
}

func (client *Client) writePump(ctx context.Context) {
	pingTicker := time.NewTicker(pingPeriod)
	pingStop := make(chan struct{})
	var text []byte
	var err error
	go client.runPing(pingTicker, pingStop)
	defer func() {
		if err := recover(); err != nil {
			return
		}
		client.conn.Close()
		close(pingStop)
		pingTicker.Stop()
		log.Info("client write pump closed")
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-client.outChan:
			text, err = json.Marshal(msg)
			if err != nil {
				log.Infof("encode message error, err: %v", err)
				continue
			}
			err = client.conn.WriteMessage(websocket.TextMessage, text)
			if err != nil {
				log.Infof("client write message error, err: %v", err)
				return
			}
		}
	}
}

func (client *Client) runPing(pingTicker *time.Ticker, stop chan struct{}) {
	defer func() {
		if err := recover(); err != nil {
			return
		}
		log.Info("writeAndPingMsg closed")
	}()
	for {
		select {
		case <-pingTicker.C:
			ddl := time.Now().Add(writeWait)
			client.conn.SetWriteDeadline(ddl)
			if err := client.conn.WriteControl(websocket.PingMessage, []byte("ping"), ddl); err != nil {
				log.Infof("client write ping error: %v", err)
				client.conn.Close()
				return
			}
		case <-stop:
			return
		}
	}
}
