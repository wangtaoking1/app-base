// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka_confluent

import (
	"context"
	"time"
)

// Header represents a kafka message header.
type Header struct {
	Key   string
	Value []byte
}

// Message is the public message type for handlers and producers.
type Message struct {
	ID      int64
	Key     string
	Value   []byte
	Headers []Header
	Time    time.Time
}

// rawMessage is the internal representation of a consumed kafka message.
// It carries partition/offset info needed for offset commits.
type rawMessage struct {
	key       []byte
	value     []byte
	headers   []Header
	timestamp time.Time
	topic     string
	partition int32
	offset    int64
}

// AuthType is the type of kafka authentication.
type AuthType string

const (
	AuthTypeRaw  AuthType = "raw"
	AuthTypeSASL AuthType = "sasl"
)

// SASLOptions holds credentials for SASL PLAIN authentication.
type SASLOptions struct {
	Username string
	Password string
}

// RequiredAcks controls broker acknowledgement behavior for producers.
type RequiredAcks int

const (
	// RequireNone does not wait for any broker acknowledgement.
	RequireNone RequiredAcks = 0
	// RequireOne waits for the leader broker to acknowledge.
	RequireOne RequiredAcks = 1
	// RequireAll waits for all in-sync replicas to acknowledge.
	RequireAll RequiredAcks = -1
)

// CompressionType is the compression algorithm for producer messages.
type CompressionType string

const (
	CompressionNone   CompressionType = "none"
	CompressionGzip   CompressionType = "gzip"
	CompressionSnappy CompressionType = "snappy"
	CompressionLz4    CompressionType = "lz4"
	CompressionZstd   CompressionType = "zstd"
)

// StartOffset determines where a new consumer group starts reading.
type StartOffset int64

const (
	// OffsetEarliest reads from the beginning of the topic.
	OffsetEarliest StartOffset = -2
	// OffsetLatest reads only new messages (default).
	OffsetLatest StartOffset = -1
)

func (o StartOffset) toConfigString() string {
	switch o {
	case OffsetEarliest:
		return "earliest"
	default:
		return "latest"
	}
}

// MessageHandler processes a single kafka message.
type MessageHandler func(ctx context.Context, msg *Message) error

// ErrorMessageHandler is called when a message fails after all retries.
type ErrorMessageHandler func(ctx context.Context, msg *Message, lastErr error) error
