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
	pingInterval     = 10 * time.Second
	writeTimeout     = 10 * time.Second
	heartbeatTimeout = 30 * time.Second
	writeBufferSize  = 100
)

type clientPeer struct {
	clientId   string
	dispatcher Dispatcher
	conn       *websocket.Conn
	writeCh    chan *Message

	stopCh chan struct{}
}

func newPeer(id string, dispatcher Dispatcher, conn *websocket.Conn) *clientPeer {
	svc := &clientPeer{
		clientId:   id,
		dispatcher: dispatcher,
		conn:       conn,
		writeCh:    make(chan *Message, writeBufferSize),
		stopCh:     make(chan struct{}),
	}
	return svc
}

func (p *clientPeer) Run(ctx context.Context) {
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		p.pingLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		p.readLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		p.writeLoop(ctx)
	}()
	wg.Wait()
	_ = p.conn.Close()
	log.Infof("Client peer closed, id: %v", p.clientId)
}

func (p *clientPeer) pingLoop(ctx context.Context) {
	_ = p.conn.SetReadDeadline(time.Now().Add(heartbeatTimeout))
	p.conn.SetPongHandler(func(string) error {
		return p.conn.SetReadDeadline(time.Now().Add(heartbeatTimeout))
	})

	pingTicker := time.NewTicker(pingInterval)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-pingTicker.C:
			if err := p.conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(writeTimeout)); err != nil {
				log.Errorf("Write ping message error: %v", err)
				return
			}
		}
	}
}

func (p *clientPeer) readLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		default:
			messageType, message, err := p.conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err) {
					log.Errorf("Connect unexpected close: %v", err)
					close(p.stopCh)
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
			p.handleMessage(ctx, msg)
		}
	}
}

func (p *clientPeer) writeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case msg := <-p.writeCh:
			text, err := json.Marshal(msg)
			if err != nil {
				log.Errorf("Marshal message error: %v", err)
				continue
			}
			err = p.conn.WriteMessage(websocket.TextMessage, text)
			if err != nil {
				log.Errorf("Write message error: %v", err)
				close(p.stopCh)
				return
			}
		}
	}
}

func (p *clientPeer) handleMessage(ctx context.Context, msg *Message) {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("Handle message panic: %v", err)
			return
		}
	}()

	p.dispatcher.Dispatch(ctx, p, msg)
}

func (p *clientPeer) Write(ctx context.Context, message *Message) error {
	select {
	case <-ctx.Done():
		return nil
	case p.writeCh <- message:
		return nil
	}
}
