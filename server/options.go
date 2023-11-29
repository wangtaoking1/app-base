// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package server

import (
	"fmt"
	"net"
	"strconv"

	"github.com/spf13/pflag"
)

// Options contains configuration options for api server.
type Options struct {
	Healthz     bool     `json:"healthz"     mapstructure:"healthz"`
	Middlewares []string `json:"middlewares" mapstructure:"middlewares"`
	Profiling   bool     `json:"profiling"   mapstructure:"profiling"`
	Metrics     bool     `json:"metrics"     mapstructure:"metrics"`

	HTTP  *HTTPOptions  `json:"http"  mapstructure:"http"`
	HTTPS *HTTPSOptions `json:"https" mapstructure:"https"`
}

// NewOptions return a new options for server.
func NewOptions() *Options {
	return &Options{
		Healthz:     true,
		Middlewares: []string{"requestid"},
		Profiling:   false,
		Metrics:     false,

		HTTP: &HTTPOptions{
			BindAddress: "127.0.0.1",
			BindPort:    8080,
		},
		HTTPS: &HTTPSOptions{
			Enabled:     false,
			BindAddress: "127.0.0.1",
			BindPort:    8443,
		},
	}
}

func (o *Options) Validate() []error {
	var errs []error
	errs = append(errs, o.HTTP.Validate()...)
	errs = append(errs, o.HTTPS.Validate()...)

	return errs
}

func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&o.Healthz, "server.healthz", o.Healthz, ""+
		"Enable self readiness check and install /healthz router.")
	fs.StringSliceVar(&o.Middlewares, "server.middlewares", o.Middlewares, ""+
		"List of middlewares allowed for server, comma separated. "+
		"If this list is empty default middlewares will be used.")
	fs.BoolVar(&o.Profiling, "server.profiling", o.Profiling, "Enable profiling for server"+
		"If enabled, you can debug profiling on /debug/pprof/xxx path")
	fs.BoolVar(&o.Metrics, "server.metrics", o.Metrics, "Enable prometheus metrics for server. "+
		"If enabled, you can download metrics on /metrics path")

	o.HTTP.AddFlags(fs)
	o.HTTPS.AddFlags(fs)
}

// HTTPOptions contains configuration for http server.
type HTTPOptions struct {
	BindAddress string `json:"bind-address" mapstructure:"bind-address"`
	BindPort    int    `json:"bind-port"    mapstructure:"bind-port"`
}

func (o *HTTPOptions) Validate() []error {
	var errs []error
	if o.BindPort <= 0 || o.BindPort > 65535 {
		errs = append(
			errs,
			fmt.Errorf("--server.http.bind-port %v must be between 1 and 65535", o.BindPort),
		)
	}

	return errs
}

func (o *HTTPOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.BindAddress, "server.http.bind-address", o.BindAddress, ""+
		"The IP address on which to serve the --server.http.bind-port "+
		"(set to 0.0.0.0 for all IPv4 interfaces and :: for all IPv6 interfaces).")
	fs.IntVar(&o.BindPort, "server.http.bind-port", o.BindPort, ""+
		"The port on which to serve unsecured, unauthenticated access. It is assumed "+
		"that firewall rules are set up such that this port is not reachable from outside of "+
		"the deployed machine.")
}

// Address join host IP address and host port number into an address string, like: 0.0.0.0:8443.
func (o *HTTPOptions) Address() string {
	return net.JoinHostPort(o.BindAddress, strconv.Itoa(o.BindPort))
}

func (o *HTTPOptions) healthzAddr() string {
	if o.BindAddress == "0.0.0.0" {
		return net.JoinHostPort("127.0.0.1", strconv.Itoa(o.BindPort))
	}

	return o.Address()
}

// HTTPSOptions contains configuration for https server.
type HTTPSOptions struct {
	Enabled     bool       `json:"enabled"      mapstructure:"enabled"`
	BindAddress string     `json:"bind-address" mapstructure:"bind-address"`
	BindPort    int        `json:"bind-port"    mapstructure:"bind-port"`
	TLS         TLSOptions `json:"tls"          mapstructure:"tls"`
}

// TLSOptions contains configuration for TLS.
type TLSOptions struct {
	// CertFile is a file containing a PEM-encoded certificate, and possibly the complete certificate chain
	CertFile string `json:"cert-file"        mapstructure:"cert-file"`
	// KeyFile is a file containing a PEM-encoded private key for the certificate specified by CertFile
	KeyFile string `json:"private-key-file" mapstructure:"private-key-file"`
}

func (o *HTTPSOptions) Validate() []error {
	if !o.Enabled {
		return nil
	}

	var errs []error
	if o.BindPort <= 0 || o.BindPort > 65535 {
		errs = append(
			errs,
			fmt.Errorf("--server.https.bind-port %v must be between 1 and 65535", o.BindPort),
		)
	}
	if len(o.TLS.CertFile) == 0 || len(o.TLS.KeyFile) == 0 {
		errs = append(errs, fmt.Errorf("--server.https.tls must be specified when https enabled"))
	}

	return errs
}

func (o *HTTPSOptions) AddFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&o.Enabled, "server.https.enabled", o.Enabled, "Enable the secure https server.")
	fs.StringVar(&o.BindAddress, "server.https.bind-address", o.BindAddress, ""+
		"The IP address on which to listen for the --server.https.bind-port port. The "+
		"associated interface(s) must be reachable by the rest of the engine, and by CLI/web "+
		"clients. If blank, all interfaces will be used (0.0.0.0 for all IPv4 interfaces and :: for all IPv6 interfaces).")
	fs.IntVar(&o.BindPort, "server.https.bind-port", o.BindPort, ""+
		"The port on which to serve secured access.")
	fs.StringVar(&o.TLS.CertFile, "server.https.tls.cert-file", o.TLS.CertFile, ""+
		"The tls cert file for the https server")
	fs.StringVar(&o.TLS.KeyFile, "server.https.tls.private-key-file", o.TLS.KeyFile, ""+
		"The tls private key file for the https server")
}

// Address join host IP address and host port number into an address string, like: 0.0.0.0:8443.
func (o *HTTPSOptions) Address() string {
	return net.JoinHostPort(o.BindAddress, strconv.Itoa(o.BindPort))
}
