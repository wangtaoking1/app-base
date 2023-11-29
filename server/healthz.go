// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/wangtaoking1/app-base/errors"
	"github.com/wangtaoking1/app-base/log"
)

const (
	healthzPath = "/healthz"
)

func (s *apiServer) addHealthzRouter() {
	s.GET(healthzPath, func(c *gin.Context) {
		c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})
}

func (s *apiServer) healthCheck() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Ping the server to make sure the router is working.
	if err := s.ping(ctx); err != nil {
		return errors.WithMessage(err, "healthz check failed")
	}

	return nil
}

// ping pings the http server to make sure the router is working.
func (s *apiServer) ping(ctx context.Context) error {
	url := fmt.Sprintf("http://%s%s", s.options.HTTP.healthzAddr(), healthzPath)

	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			log.Debug("The router has been deployed successfully.")

			_ = resp.Body.Close()

			return nil
		}

		// Sleep for a second to try the next ping.
		log.Debug("Waiting for the router deploy, retry in 1 second.")
		time.Sleep(1 * time.Second)

		select {
		case <-ctx.Done():
			log.Fatal("Can not ping http server within the specified time interval.")
		default:
		}
	}
}
