// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package websocket

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/wangtaoking1/app-base/log"
)

type Server struct {
	opts *Options

	httpServer *http.Server
	dispatcher *Dispatcher
	upgrader   websocket.Upgrader
}

func NewServer(dispatcher *Dispatcher, opts *Options) *Server {
	return &Server{
		httpServer: &http.Server{Addr: fmt.Sprintf("%s:%d", opts.BindAddress, opts.BindPort)},
		dispatcher: dispatcher,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  opts.ReadBufferSize,
			WriteBufferSize: opts.WriteBufferSize,
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
			EnableCompression: opts.Compression,
		},
	}
}

func (s *Server) HandleHealth(writer http.ResponseWriter, request *http.Request) {
	writer.WriteHeader(200)
	writer.Write([]byte("")) //nolint
}

func (s *Server) Run() {
	http.HandleFunc("/health_check", s.HandleHealth)
	http.HandleFunc("/ws", s.handleStream)
	log.Infow("http server start", "address", s.httpServer.Addr)
	defer log.Info("http server closed")
	err := s.httpServer.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Errorf("http server error: %v", err)
	}
}

func (s *Server) handleStream(writer http.ResponseWriter, request *http.Request) {
	id := request.URL.Query().Get("uuid")
	if len(id) == 0 {
		id = uuid.New().String()
	}
	ip := request.Header.Get("True-Client-IP")
	if len(ip) == 0 {
		ip = request.RemoteAddr
	}
	log.Infow("request",
		"client_id", id,
		"url", request.URL.Path,
		"real_ip", ip,
	)
	conn, err := s.upgrader.Upgrade(writer, request, nil)
	if err != nil {
		log.Infof("stream request error: %v", err)
		return
	}
	client := newClient(s.dispatcher, conn)
	client.run(request.Context())
}
