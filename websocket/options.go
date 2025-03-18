// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package websocket

import (
	"fmt"

	"github.com/spf13/pflag"
)

// Options contains configuration options.
type Options struct {
	BindAddress     string `json:"bind-address" mapstructure:"bind-address"`
	BindPort        int    `json:"bind-port"    mapstructure:"bind-port"`
	ReadBufferSize  int    `json:"read-buffer-size" mapstructure:"read-buffer-size"`
	WriteBufferSize int    `json:"write-buffer-size" mapstructure:"write-buffer-size"`
	Compression     bool   `json:"compression" mapstructure:"compression"`
}

// NewOptions return a new options for server.
func NewOptions() *Options {
	return &Options{
		BindAddress:     "127.0.0.1",
		BindPort:        6060,
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		Compression:     true,
	}
}

func (o *Options) Validate() []error {
	var errs []error
	if o.BindPort <= 0 || o.BindPort > 65535 {
		errs = append(errs, fmt.Errorf("--websocket.bind-port %v must be between 1 and 65535", o.BindPort))
	}
	return errs
}

func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.BindAddress, "websocket.bind-address", o.BindAddress, "The IP address on which to serve the websocket server")
	fs.IntVar(&o.BindPort, "websocket.bind-port", o.BindPort, "The port on which to serve the websocket server")
	fs.IntVar(&o.ReadBufferSize, "websocket.read-buffer-size", o.ReadBufferSize, "The byte size of websocket read buffer")
	fs.IntVar(&o.WriteBufferSize, "websocket.write-buffer-size", o.WriteBufferSize, "The byte size of websocket write buffer")
	fs.BoolVar(&o.Compression, "websocket.compression", o.Compression, "Enable compression for websocket message")
}
