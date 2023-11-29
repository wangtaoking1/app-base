// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package grpc

import (
	"fmt"
	"net"
	"strconv"

	"github.com/spf13/pflag"
)

// Options contains configuration options for grpc server.
type Options struct {
	BindAddress string     `json:"bind-address" mapstructure:"bind-address"`
	BindPort    int        `json:"bind-port"    mapstructure:"bind-port"`
	MaxMsgSize  int        `json:"max-msg-size" mapstructure:"max-msg-size"`
	TLS         TLSOptions `json:"tls"          mapstructure:"tls"`
}

// TLSOptions contains configuration for TLS.
type TLSOptions struct {
	Enabled bool `json:"enabled"          mapstructure:"enabled"`
	// CertFile is a file containing a PEM-encoded certificate, and possibly the complete certificate chain
	CertFile string `json:"cert-file"        mapstructure:"cert-file"`
	// KeyFile is a file containing a PEM-encoded private key for the certificate specified by CertFile
	KeyFile string `json:"private-key-file" mapstructure:"private-key-file"`
}

// NewOptions return a new options for server.
func NewOptions() *Options {
	return &Options{
		BindAddress: "127.0.0.1",
		BindPort:    8081,
		MaxMsgSize:  4 * 1024 * 1024,
		TLS: TLSOptions{
			Enabled: false,
		},
	}
}

// Address join host IP address and host port number into an address string, like: 0.0.0.0:80.
func (o *Options) Address() string {
	return net.JoinHostPort(o.BindAddress, strconv.Itoa(o.BindPort))
}

func (o *Options) Validate() []error {
	var errs []error
	if o.BindPort <= 0 || o.BindPort > 65535 {
		errs = append(
			errs,
			fmt.Errorf("--grpc.bind-port %v must be between 1 and 65535", o.BindPort),
		)
	}
	if o.TLS.Enabled {
		if len(o.TLS.CertFile) == 0 || len(o.TLS.KeyFile) == 0 {
			errs = append(errs, fmt.Errorf("--grpc.tls must be specified when tls enabled"))
		}
	}

	return errs
}

func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.BindAddress, "grpc.bind-address", o.BindAddress, ""+
		"The IP address on which to listen for the --grpc.bind-port port. "+
		"If blank, all interfaces will be used (0.0.0.0 for all IPv4 interfaces and :: for all IPv6 interfaces).")
	fs.IntVar(&o.BindPort, "grpc.bind-port", o.BindPort, ""+
		"The port on which grpc server.")
	fs.IntVar(&o.MaxMsgSize, "grpc.max-msg-size", o.MaxMsgSize, "grpc max message size.")

	fs.BoolVar(&o.TLS.Enabled, "grpc.tls.enabled", o.TLS.Enabled, "Enable the secure grpc server.")
	fs.StringVar(&o.TLS.CertFile, "grpc.tls.cert-file", o.TLS.CertFile, ""+
		"The tls cert file for the grpc server.")
	fs.StringVar(&o.TLS.KeyFile, "grpc.tls.private-key-file", o.TLS.KeyFile, ""+
		"The tls private key file for the grpc server.")
}
