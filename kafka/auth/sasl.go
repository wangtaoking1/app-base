// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package auth

import (
	"net"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

type saslAuthenticator struct {
	username string
	password string
}

func NewSaslAuthenticator(username string, password string) Authenticator {
	return &saslAuthenticator{
		username: username,
		password: password,
	}
}

func (a *saslAuthenticator) GetTransport(assumeRole string) kafka.RoundTripper {
	return &kafka.Transport{
		Dial: (&net.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		}).DialContext,
		SASL: plain.Mechanism{
			Username: a.username,
			Password: a.password,
		},
	}
}

func (a *saslAuthenticator) GetDialer(assumeRole string) *kafka.Dialer {
	return &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		SASLMechanism: plain.Mechanism{
			Username: a.username,
			Password: a.password,
		},
	}
}
