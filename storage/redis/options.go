// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package redis

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/spf13/pflag"
)

// Options defines options for redis cluster.
type Options struct {
	Addrs          []string      `json:"addrs"           mapstructure:"addrs"`
	Username       string        `json:"username"        mapstructure:"username"`
	Password       string        `json:"password"        mapstructure:"password"`
	Database       int           `json:"database"        mapstructure:"database"`
	MasterName     string        `json:"master-name"     mapstructure:"master-name"`
	EnableCluster  bool          `json:"enable-cluster"  mapstructure:"enable-cluster"`
	PoolSize       int           `json:"pool-size"       mapstructure:"pool-size"`
	DialTimeout    time.Duration `json:"dial-timeout"    mapstructure:"dial-timeout"`
	RequestTimeout time.Duration `json:"request-timeout" mapstructure:"request-timeout"`
	TLS            TLSOptions    `json:"tls"             mapstructure:"tls"`
}

// TLSOptions contains configuration for TLS.
type TLSOptions struct {
	Enabled bool `json:"enabled"          mapstructure:"enabled"`
	// CertFile is a file containing a PEM-encoded certificate, and possibly the complete certificate chain
	CertFile string `json:"cert-file"        mapstructure:"cert-file"`
	// KeyFile is a file containing a PEM-encoded private key for the certificate specified by CertFile
	KeyFile string `json:"private-key-file" mapstructure:"private-key-file"`
}

// NewOptions create a new redis options instance.
func NewOptions() *Options {
	return &Options{
		Addrs:          []string{"127.0.0.1:6379"},
		Username:       "",
		Password:       "",
		Database:       0,
		MasterName:     "",
		EnableCluster:  false,
		PoolSize:       100,
		DialTimeout:    0,
		RequestTimeout: 0,
		TLS: TLSOptions{
			Enabled: false,
		},
	}
}

// Validate verifies flags passed to Options.
func (o *Options) Validate() []error {
	var errs []error

	if len(o.Addrs) == 0 {
		errs = append(errs, fmt.Errorf("redis addrs can not be empty"))
	}

	if o.DialTimeout <= 0 {
		errs = append(errs, fmt.Errorf("--redis.dial-timeout cannot be negative"))
	}

	if o.RequestTimeout <= 0 {
		errs = append(errs, fmt.Errorf("--redis.request-timeout cannot be negative"))
	}

	if o.TLS.Enabled {
		if len(o.TLS.CertFile) == 0 || len(o.TLS.KeyFile) == 0 {
			errs = append(errs, fmt.Errorf("--redis.tls must be specified when tls enabled"))
		}
	}

	return errs
}

// AddFlags adds flags related to redis storage to the specified FlagSet.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringSliceVar(&o.Addrs, "redis.addrs", o.Addrs, "A set of redis address(format: 127.0.0.1:6379).")
	fs.StringVar(&o.Username, "redis.username", o.Username, "Username for access to redis service.")
	fs.StringVar(&o.Password, "redis.password", o.Password, "Optional auth password for Redis db.")
	fs.IntVar(&o.Database, "redis.database", o.Database, ""+
		"By default, the database is 0. Setting the database is not supported with redis cluster. "+
		"As such, if you have --redis.enable-cluster=true, then this value should be omitted or explicitly set to 0.")

	fs.StringVar(&o.MasterName, "redis.master-name", o.MasterName, "The name of master redis instance.")
	fs.BoolVar(&o.EnableCluster, "redis.enable-cluster", o.EnableCluster, ""+
		"If you are using Redis cluster, enable it here to enable the slots mode.")

	fs.IntVar(&o.PoolSize, "redis.pool-size", o.PoolSize, "The max size of the redis pool.")
	fs.DurationVar(&o.DialTimeout, "redis.dial-timeout", o.DialTimeout, ""+
		"Redis dial timeout in seconds.")
	fs.DurationVar(&o.RequestTimeout, "redis.request-timeout", o.RequestTimeout, ""+
		"Redis request timeout in seconds.")

	fs.BoolVar(&o.TLS.Enabled, "redis.tls.enabled", o.TLS.Enabled, "Use tls transport to connect redis cluster.")
	fs.StringVar(&o.TLS.CertFile, "redis.tls.cert-file", o.TLS.CertFile, ""+
		"The tls cert file for transport.")
	fs.StringVar(&o.TLS.KeyFile, "redis.tls.private-key-file", o.TLS.KeyFile, ""+
		"The tls private key file for transport.")
}

func (o *Options) loadTLSConfig() (*tls.Config, error) {
	if !o.TLS.Enabled {
		return nil, nil
	}

	cert, err := tls.LoadX509KeyPair(o.TLS.CertFile, o.TLS.KeyFile)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}
