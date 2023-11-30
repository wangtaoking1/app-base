// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package etcd

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/spf13/pflag"
)

// Options defines options for etcd cluster.
type Options struct {
	Endpoints      []string      `json:"endpoints"       mapstructure:"endpoints"`
	Username       string        `json:"username"        mapstructure:"username"`
	Password       string        `json:"password"        mapstructure:"password"`
	Namespace      string        `json:"namespace"       mapstructure:"namespace"`
	TLS            TLSOptions    `json:"tls"             mapstructure:"tls"`
	Timeout        time.Duration `json:"timeout"         mapstructure:"timeout"`
	RequestTimeout time.Duration `json:"request-timeout" mapstructure:"request-timeout"`
	LeaseExpire    time.Duration `json:"lease-expire"    mapstructure:"lease-expire"`
}

// TLSOptions contains configuration for TLS.
type TLSOptions struct {
	Enabled bool `json:"enabled"          mapstructure:"enabled"`
	// CertFile is a file containing a PEM-encoded certificate, and possibly the complete certificate chain
	CertFile string `json:"cert-file"        mapstructure:"cert-file"`
	// KeyFile is a file containing a PEM-encoded private key for the certificate specified by CertFile
	KeyFile string `json:"private-key-file" mapstructure:"private-key-file"`
}

// NewOptions create a new options instance.
func NewOptions() *Options {
	return &Options{
		Endpoints:      []string{"127.0.0.1:2379"},
		Timeout:        5 * time.Second,
		RequestTimeout: 2 * time.Second,
		LeaseExpire:    5 * time.Second,
		TLS: TLSOptions{
			Enabled: false,
		},
	}
}

// Validate verifies flags passed to RedisOptions.
func (o *Options) Validate() []error {
	var errs []error

	if len(o.Endpoints) == 0 {
		errs = append(errs, fmt.Errorf("etcd endpoints can not be empty"))
	}

	if o.RequestTimeout <= 0 {
		errs = append(errs, fmt.Errorf("--etcd.request-timeout cannot be negative"))
	}

	if o.TLS.Enabled {
		if len(o.TLS.CertFile) == 0 || len(o.TLS.KeyFile) == 0 {
			errs = append(errs, fmt.Errorf("--etcd.tls must be specified when tls enabled"))
		}
	}

	return errs
}

// AddFlags adds flags related to redis storage for a specific APIServer to the specified FlagSet.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringSliceVar(&o.Endpoints, "etcd.endpoints", o.Endpoints, "Endpoints of etcd cluster.")
	fs.StringVar(&o.Username, "etcd.username", o.Username, "Username of etcd cluster.")
	fs.StringVar(&o.Password, "etcd.password", o.Password, "Password of etcd cluster.")
	fs.DurationVar(&o.Timeout, "etcd.timeout", o.Timeout, "Etcd dial timeout in seconds.")
	fs.DurationVar(&o.RequestTimeout, "etcd.request-timeout", o.RequestTimeout, "Etcd request timeout in seconds.")
	fs.DurationVar(&o.LeaseExpire, "etcd.lease-expire", o.LeaseExpire, "Etcd expire timeout in seconds.")
	fs.BoolVar(&o.TLS.Enabled, "etcd.tls.enabled", o.TLS.Enabled, "Use tls transport to connect etcd cluster.")
	fs.StringVar(&o.TLS.CertFile, "etcd.tls.cert-file", o.TLS.CertFile, ""+
		"The tls cert file for transport.")
	fs.StringVar(&o.TLS.KeyFile, "etcd.tls.private-key-file", o.TLS.KeyFile, ""+
		"The tls private key file for transport.")
	fs.StringVar(&o.Namespace, "etcd.namespace", o.Namespace, "Etcd storage namespace.")
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
