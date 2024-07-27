// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package auth

import (
	"github.com/segmentio/kafka-go"
)

type AuthType string

const (
	AuthTypeRaw AuthType = "raw"
)

type Authenticator interface {
	// GetTransport returns a kafka transport with the credentials of specified platform.
	GetTransport(assumeRole string) kafka.RoundTripper
	// GetDialer returns a kafka dialer with the credentials of specified platform.
	GetDialer(assumeRole string) *kafka.Dialer
}

type rawAuthenticator struct{}

func NewRawAuthenticator() Authenticator {
	return &rawAuthenticator{}
}

func (a *rawAuthenticator) GetTransport(assumeRole string) kafka.RoundTripper {
	return kafka.DefaultTransport
}

func (a *rawAuthenticator) GetDialer(assumeRole string) *kafka.Dialer {
	return kafka.DefaultDialer
}
