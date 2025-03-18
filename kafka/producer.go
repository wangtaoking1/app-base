// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/wangtaoking1/app-base/kafka/auth"
	"github.com/wangtaoking1/app-base/log"
)

type Message struct {
	ID      int64
	Key     string
	Value   []byte
	Headers []kafka.Header
}

type Producer interface {
	SendMessage(ctx context.Context, msgs ...Message) error
	SendMessageWithTopic(ctx context.Context, topic string, msgs ...Message) error
	Close()
}

type ProducerOptions struct {
	// AuthType is the type of authentication.
	// Default: raw.
	AuthType auth.AuthType

	// SASLOptions is the options for SASL authentication.
	SASLOptions *SASLOptions

	// Number of acknowledges from partition replicas required before receiving
	// a response to a produce request.
	// Default: RequireNone.
	RequireAcks kafka.RequiredAcks

	// Async indicates whether the producer should send messages asynchronously.
	// Default: false.
	Async bool

	// Limit on how many messages will be buffered before being sent to a
	// partition.
	//
	// The default is to use a target batch size of 100 messages.
	BatchSize int

	// Time limit on how often incomplete message batches will be flushed to
	// kafka.
	//
	// The default is to flush at least every second.
	BatchTimeout time.Duration

	// Balancer is the balancer strategy used to distribute messages to partitions.
	// Default: kafka.RoundRobin.
	Balancer kafka.Balancer

	// Compression is the compression algorithm used to compress messages.
	// Default: kafka.Gzip.
	Compression kafka.Compression
}

type SASLOptions struct {
	Username string
	Password string
}

func (o *ProducerOptions) SetDefaults() error {
	if o.AuthType == "" {
		o.AuthType = auth.AuthTypeRaw
	}
	if o.Balancer == nil {
		o.Balancer = &kafka.RoundRobin{}
	}
	if o.Compression == 0 {
		o.Compression = kafka.Gzip
	}
	return nil
}

type producer struct {
	brokers string
	topic   string
	options *ProducerOptions

	author auth.Authenticator
	writer *kafka.Writer
}

// NewProducer creates a new producer with default options.
func NewProducer(brokers string, topic string) (Producer, error) {
	return NewProducerWithOptions(brokers, topic, &ProducerOptions{})
}

// NewProducerWithOptions creates a new producer with custom options.
func NewProducerWithOptions(brokers string, topic string, opts *ProducerOptions) (Producer, error) {
	if err := opts.SetDefaults(); err != nil {
		return nil, err
	}

	p := &producer{
		brokers: brokers,
		topic:   topic,
		options: opts,
	}

	switch opts.AuthType {
	case auth.AuthTypeSASL:
		if opts.SASLOptions == nil {
			return nil, errors.New("SASL options must be specified for SASL authentication")
		}
		p.author = auth.NewSaslAuthenticator(opts.SASLOptions.Username, opts.SASLOptions.Password)
	default:
		p.author = auth.NewRawAuthenticator()
	}
	p.writer = &kafka.Writer{
		Transport:    p.author.GetTransport(""),
		Addr:         kafka.TCP(strings.Split(brokers, ",")...),
		Topic:        topic,
		Balancer:     opts.Balancer,
		RequiredAcks: opts.RequireAcks,
		Async:        opts.Async,
		BatchSize:    opts.BatchSize,
		BatchTimeout: opts.BatchTimeout,
		Compression:  opts.Compression,
	}

	return p, nil
}

func (p *producer) Close() {
	if p.writer == nil {
		return
	}
	if err := p.writer.Close(); err != nil {
		log.Errorw("Error close kafka producer", "error", err)
	}
}

func (p *producer) SendMessage(ctx context.Context, msgs ...Message) error {
	if p.topic == "" {
		return errors.New("no specified topic in Producer")
	}
	kafkaMsgs := make([]kafka.Message, 0, len(msgs))
	for _, msg := range msgs {
		kafkaMsgs = append(kafkaMsgs, kafka.Message{
			Key:     []byte(msg.Key),
			Value:   msg.Value,
			Headers: msg.Headers,
		})
	}
	return p.writer.WriteMessages(ctx, kafkaMsgs...)
}

func (p *producer) SendMessageWithTopic(ctx context.Context, topic string, msgs ...Message) error {
	if p.topic != "" {
		return errors.New("topic must not be specified for both Producer and Message")
	}
	kafkaMsgs := make([]kafka.Message, 0, len(msgs))
	for _, msg := range msgs {
		kafkaMsgs = append(kafkaMsgs, kafka.Message{
			Topic:   topic,
			Key:     []byte(msg.Key),
			Value:   msg.Value,
			Headers: msg.Headers,
		})
	}
	return p.writer.WriteMessages(ctx, kafkaMsgs...)
}
