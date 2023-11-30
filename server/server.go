// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package server

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	ginprometheus "github.com/zsais/go-gin-prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/wangtaoking1/app-base/log"
	"github.com/wangtaoking1/app-base/server/middleware"
)

// APIServer is the interface of the api server.
type APIServer interface {
	// Setup setups the server engine, like custom routers or middlewares.
	// Setup should be called before Run.
	Setup(SetupFunc) error
	// Run starts the api server engine.
	Run() error
	// Close shutdowns the api server engine.
	Close()
}

// SetupFunc is the func used to set up the engine.
type SetupFunc func(g *gin.Engine) error

type apiServer struct {
	*gin.Engine

	options *Options

	httpServer, httpsServer *http.Server
}

// New returns a new api server instance.
func New(options *Options) APIServer {
	if options == nil {
		return nil
	}

	// use release mode derectly
	gin.SetMode(gin.ReleaseMode)

	s := &apiServer{
		options: options,
		Engine:  gin.New(),
	}

	s.initServer()

	return s
}

func (s *apiServer) initServer() {
	s.setupGlobalMiddlewares()
	s.setupGlobalRouters()
}

func (s *apiServer) setupGlobalMiddlewares() {
	installed := make([]string, 0, len(s.options.Middlewares))
	for _, m := range s.options.Middlewares {
		mw := middleware.Get(m)
		if mw == nil {
			log.Warnf("Middleware %s can not found", m)

			continue
		}
		installed = append(installed, m)
		s.Use(mw)
	}
	if len(installed) != 0 {
		log.Infof("Installed middlewares: %s", strings.Join(installed, ","))
	}
}

func (s *apiServer) setupGlobalRouters() {
	if s.options.Healthz {
		s.addHealthzRouter()
	}

	if s.options.Metrics {
		prometheus := ginprometheus.NewPrometheus("gin")
		prometheus.Use(s.Engine)
	}

	if s.options.Profiling {
		pprof.Register(s.Engine)
	}
}

func (s *apiServer) Setup(setupFunc SetupFunc) error {
	if setupFunc == nil {
		return nil
	}
	if err := setupFunc(s.Engine); err != nil {
		return err
	}

	return nil
}

//nolint:gosec
func (s *apiServer) Run() error {
	var eg errgroup.Group

	// Initializing the http server.
	// For scalability, use custom HTTP Server mode here
	s.httpServer = &http.Server{
		Addr:    s.options.HTTP.Address(),
		Handler: s,
	}
	eg.Go(func() error {
		log.Infof("Start to listening on http server: %s", s.options.HTTP.Address())

		if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err.Error())

			return err
		}
		log.Infof("Server on %s stopped", s.options.HTTP.Address())

		return nil
	})

	// Initializing the https server.
	if s.options.HTTPS.Enabled {
		s.httpsServer = &http.Server{
			Addr:    s.options.HTTPS.Address(),
			Handler: s,
		}
		eg.Go(func() error {
			key, cert := s.options.HTTPS.TLS.KeyFile, s.options.HTTPS.TLS.CertFile
			if key == "" || cert == "" {
				return nil
			}

			log.Infof("Start to listening on https server: %s", s.options.HTTPS.Address())

			if err := s.httpsServer.ListenAndServeTLS(cert, key); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Fatal(err.Error())

				return err
			}
			log.Infof("Server on %s stopped", s.options.HTTPS.Address())

			return nil
		})
	}

	// Do health check
	if s.options.Healthz {
		if err := s.healthCheck(); err != nil {
			log.Fatal(err.Error())
		}
	}

	if err := eg.Wait(); err != nil {
		log.Fatal(err.Error())
	}

	return nil
}

func (s *apiServer) Close() {
	// The context is used to conform the server it has 10 seconds to finish
	// the requests handling currently
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			log.Warnf("Failed to shutdown http server: %s", err.Error())
		}
		log.Infof("HTTP server on %s stopped", s.options.HTTP.Address())
	}

	if s.httpsServer != nil {
		if err := s.httpsServer.Shutdown(ctx); err != nil {
			log.Warnf("Failed to shutdown https server: %s", err.Error())
		}
		log.Infof("HTTPS server on %s stopped", s.options.HTTPS.Address())
	}
}
