// Copyright 2024 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/wangtaoking1/app-base/kafka/auth"
	"github.com/wangtaoking1/app-base/log"
	"github.com/wangtaoking1/app-base/utils"
)

const (
	// errorRetryInterval is the interval to retry when brokers error occurs.
	errorRetryInterval = 5 * time.Second

	msgRetryLimit    = 3
	msgRetryInterval = 100 * time.Millisecond
)

// Consumer is the interface of kafka consumer.
type Consumer interface {
	Run(ctx context.Context)
}

type (
	MessageHandler      func(ctx context.Context, msg *Message) error
	ErrorMessageHandler func(ctx context.Context, msg *Message, lastErr error) error
)

type ConsumerOptions struct {
	// AuthType is the type of authentication.
	// Default: raw.
	AuthType auth.AuthType
	// ErrHandler is the handler for error message.
	// Default: nil
	ErrHandler ErrorMessageHandler
	// OrderdMode is the order mode of consuming messages. If true, consume messages with the same key
	// in order, otherwise consume all messages in random order.
	// Default: false
	OrderedMode bool
	// ParalSize is the number of parallel workers to consume messages.
	// Default: 5
	ParalSize int
	// CacheSize is the size of cache queue.
	// Default: 100
	CacheSize int
	// FetchMinBytes is the minimum number of bytes to fetch in one batch from brokers.
	// Default: 10KB
	FetchMinBytes int
	// FetchMaxBytes is the maximum number of bytes to fetch in one batch from brokers.
	// Default: 10MB
	FetchMaxBytes int
	// FetchMaxWait is the maximum time to wait in one batch from brokers.
	// Default: 1s
	FetchMaxWait time.Duration
	// StartOffset is the beginning offset when no committed offset in partition. If
	// non-zero, it must be set to one of kafka.FirstOffset(-2) or kafka.LastOffset(-1).
	// Default: kafka.LastOffset
	StartOffset int64
}

func (o *ConsumerOptions) SetDefaults() error {
	if o.AuthType == "" {
		o.AuthType = auth.AuthTypeRaw
	}
	if o.ParalSize == 0 {
		o.ParalSize = 5
	}
	if o.CacheSize == 0 {
		o.CacheSize = 100
	}
	if o.FetchMinBytes == 0 {
		o.FetchMinBytes = 1024 * 10 // 10KB
	}
	if o.FetchMaxBytes == 0 {
		o.FetchMaxBytes = 1024 * 1024 * 10 // 10MB
	}
	if o.FetchMaxWait == 0 {
		o.FetchMaxWait = 1 * time.Second
	}
	if o.StartOffset == 0 {
		o.StartOffset = kafka.LastOffset
	}
	if o.StartOffset != kafka.FirstOffset && o.StartOffset != kafka.LastOffset {
		return errors.New("invalid start offset of consumer")
	}
	return nil
}

type consumer struct {
	options *ConsumerOptions
	brokers string
	topic   string
	groupID string
	handler MessageHandler

	reader *kafka.Reader

	author       auth.Authenticator
	running      *atomic.Bool
	consumeQueue ConsumeQueue
}

// NewConsumer creates a new consumer with default options.
func NewConsumer(brokers, topic, groupID string, handler MessageHandler) (Consumer, error) {
	return NewConsumerWithOptions(brokers, topic, groupID, handler, &ConsumerOptions{})
}

// NewConsumerWithOptions creates a new consumer with custom options.
func NewConsumerWithOptions(
	brokers, topic, groupID string,
	handler MessageHandler,
	opts *ConsumerOptions,
) (Consumer, error) {
	if err := opts.SetDefaults(); err != nil {
		return nil, err
	}

	c := &consumer{
		options: opts,
		brokers: brokers,
		topic:   topic,
		groupID: groupID,
		handler: handler,

		running: &atomic.Bool{},
	}

	if opts.AuthType == auth.AuthTypeRaw {
		c.author = auth.NewRawAuthenticator()
	}

	c.reader = kafka.NewReader(kafka.ReaderConfig{
		Dialer:      c.author.GetDialer(""),
		Brokers:     strings.Split(brokers, ","),
		Topic:       topic,
		GroupID:     groupID,
		MinBytes:    c.options.FetchMinBytes,
		MaxBytes:    c.options.FetchMaxBytes,
		MaxWait:     c.options.FetchMaxWait,
		StartOffset: c.options.StartOffset,
	})
	if opts.OrderedMode {
		c.consumeQueue = newOrderedQueue(opts.CacheSize, opts.ParalSize, c.handleMessage, c.commitOffset)
	} else {
		c.consumeQueue = newUnorderedQueue(opts.CacheSize, opts.ParalSize, c.handleMessage, c.commitOffset)
	}

	return c, nil
}

func (c *consumer) Run(ctx context.Context) {
	c.running.Store(true)

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		c.consume(ctx)
	}()
	go func() {
		defer wg.Done()
		c.consumeQueue.Run(ctx)
	}()

	<-ctx.Done()
	c.running.Store(false)
	wg.Wait()
	c.close()
}

func (c *consumer) close() {
	if err := c.reader.Close(); err != nil {
		log.Warn("Failed to close kafka reader", "error", err)
	}
	log.Info("Consumer stopped", "topic", c.topic)
}

func (c *consumer) consume(ctx context.Context) {
	for c.running.Load() {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			log.Error("Error fetch kafka message", "topic", c.topic, "error", err)
			time.Sleep(errorRetryInterval)
			continue
		}
		c.consumeQueue.Add(&msg)
	}
}

func (c *consumer) handleMessage(ctx context.Context, m *kafka.Message) {
	msg := &Message{
		Key:     string(m.Key),
		Value:   m.Value,
		Headers: m.Headers,
	}

	// Retry after interval time if handle error.
	err := utils.Retry(msgRetryLimit, msgRetryInterval, func() error {
		return c.handler(ctx, msg)
	})
	if err != nil {
		log.Error("Error handle message from kafka", "error", err, "key", msg.Key,
			"body", string(msg.Value))
		if !errors.Is(err, utils.NotRetryErr) && c.options.ErrHandler != nil {
			_ = c.options.ErrHandler(ctx, msg, err)
		}
		return
	}
}

func (c *consumer) commitOffset(ctx context.Context, messages []kafka.Message) error {
	if c.reader == nil {
		return nil
	}
	if err := c.reader.CommitMessages(ctx, messages...); err != nil {
		log.Error("Error commit offset to kafka", "topic", c.topic, "error", err.Error())
		return err
	}
	return nil
}
