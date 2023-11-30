// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package grpc

import (
	"net"

	"github.com/wangtaoking1/app-base/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
)

// Server is the interface of grpc server.
type Server interface {
	// Setup setups the grpc server. Setup should be called before Run.
	Setup(SetupFunc) error
	// Run starts the grpc server.
	Run() error
	// Close shutdowns the grpc server.
	Close()
}

// SetupFunc is the func used to set up the engine.
type SetupFunc func(s *grpc.Server) error

type server struct {
	*grpc.Server

	options *Options
}

// New returns a new grpc server.
func New(opts *Options) Server {
	var grpcOptions []grpc.ServerOption
	if opts.TLS.Enabled {
		creds, err := credentials.NewServerTLSFromFile(opts.TLS.CertFile, opts.TLS.KeyFile)
		if err != nil {
			log.Fatalf("Failed to load tls credentials: %v", err)
		}
		grpcOptions = append(grpcOptions, grpc.Creds(creds))
	}
	grpcOptions = append(grpcOptions, grpc.MaxRecvMsgSize(opts.MaxMsgSize))

	grpcServer := grpc.NewServer(grpcOptions...)
	reflection.Register(grpcServer)

	return &server{
		Server:  grpcServer,
		options: opts,
	}
}

func (s *server) Setup(setupFunc SetupFunc) error {
	if setupFunc == nil {
		return nil
	}
	if err := setupFunc(s.Server); err != nil {
		return err
	}

	return nil
}

func (s *server) Run() error {
	log.Infof("Start to listening on grpc server: %s", s.options.Address())

	listen, err := net.Listen("tcp", s.options.Address())
	if err != nil {
		log.Fatalf("Failed to create grpc listen: %s", err.Error())
	}

	if err := s.Serve(listen); err != nil {
		log.Fatalf("Failed to start grpc server: %s", err.Error())
	}
	log.Infof("Server on %s stopped", s.options.Address())

	return nil
}

func (s *server) Close() {
	s.GracefulStop()

	log.Infof("GRPC server on %s stopped", s.options.Address())
}
