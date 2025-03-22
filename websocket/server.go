// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package websocket

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/wangtaoking1/app-base/log"
)

type Server struct {
	opts       *Options
	dispatcher Dispatcher

	httpServer *http.Server
	upgrader   websocket.Upgrader
}

// NewServer creates a new websocket Server instance.
func NewServer(dispatcher Dispatcher, opts *Options) *Server {
	return &Server{
		opts:       opts,
		dispatcher: dispatcher,
		httpServer: &http.Server{Addr: fmt.Sprintf("%s:%d", opts.BindAddress, opts.BindPort)},
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

func (s *Server) handleHealthCheck(writer http.ResponseWriter, _ *http.Request) {
	writer.WriteHeader(200)
	writer.Write([]byte("")) //nolint
}

func (s *Server) Run(ctx context.Context) {
	http.HandleFunc("/health_check", s.handleHealthCheck)
	http.HandleFunc("/ws", s.handleStream)
	log.Infow("HTTP server start", "address", s.httpServer.Addr)
	err := s.httpServer.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Errorf("HTTP server error: %v", err)
	}
}

func (s *Server) Shutdown(ctx context.Context) {
	if err := s.httpServer.Shutdown(ctx); err != nil {
		log.Errorf("HTTP server shutdown error: %v", err)
	}
	log.Infow("HTTP server shutdown")
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
	log.Infow("Request", "client_id", id, "url", request.URL.Path, "real_ip", ip)
	conn, err := s.upgrader.Upgrade(writer, request, nil)
	if err != nil {
		log.Infof("Upgrade stream request error: %v", err)
		return
	}
	peer := newPeer(id, s.dispatcher, conn)
	peer.Run(request.Context())
}
