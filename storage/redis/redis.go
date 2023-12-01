// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package redis

import (
	"context"
	"sync"

	"github.com/go-redis/redis/v7"

	"github.com/wangtaoking1/app-base/errors"
	"github.com/wangtaoking1/app-base/log"
)

var (
	// singletonInstance is the global singleton redis client instance.
	singletonInstance redis.UniversalClient
	once              sync.Once
)

// InitInstance initialize the redis cluster client.
func InitInstance(ctx context.Context, opts *Options) error {
	if opts == nil {
		return errors.New("Options can not be nil")
	}

	once.Do(func() {
		var err error
		singletonInstance, err = newRedisClient(opts)
		if err != nil {
			log.Fatalf("Failed to init redis client: %v", err)
		}

		go shutdown(ctx)
	})

	if singletonInstance == nil {
		return errors.New("init redis instance error")
	}

	return nil
}

// newRedisClient create a new redis cluster client.
func newRedisClient(opts *Options) (redis.UniversalClient, error) {
	tlsConfig, err := opts.loadTLSConfig()
	if err != nil {
		return nil, err
	}

	universalOpts := &redis.UniversalOptions{
		Addrs:      opts.Addrs,
		MasterName: opts.MasterName,
		Username:   opts.Username,
		Password:   opts.Password,
		DB:         opts.Database,

		PoolSize:     opts.PoolSize,
		DialTimeout:  opts.DialTimeout,
		ReadTimeout:  opts.RequestTimeout,
		WriteTimeout: opts.RequestTimeout,
		TLSConfig:    tlsConfig,
	}

	var client redis.UniversalClient
	if opts.MasterName != "" {
		log.Debug("--> [REDIS] Creating sentinel-backed failover client")
		client = redis.NewFailoverClient(universalOpts.Failover())
	} else if opts.EnableCluster {
		log.Debug("--> [REDIS] Creating cluster client")
		client = redis.NewClusterClient(universalOpts.Cluster())
	} else {
		log.Debug("--> [REDIS] Creating single-node client")
		client = redis.NewClient(universalOpts.Simple())
	}

	return client, nil
}

func shutdown(ctx context.Context) {
	<-ctx.Done()

	if singletonInstance != nil {
		singletonInstance.Shutdown()
		_ = singletonInstance.Close()
	}
}
