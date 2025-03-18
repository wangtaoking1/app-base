// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/wangtaoking1/app-base/kafka/auth"
	"github.com/wangtaoking1/app-base/log"
	"github.com/wangtaoking1/app-base/utils"
)

const (
	// errorRetryInterval is the interval to retry when brokers error occurs.
	errorRetryInterval = 5 * time.Second

	defaultRetryLimit    = 3
	defaultRetryInterval = 100 * time.Millisecond

	msgFastRetryLimit    = 3
	msgFastRetryInterval = 10 * time.Millisecond
	msgSlowRetryInterval = 30 * time.Second
)

// Consumer is the interface of kafka consumer.
type Consumer interface {
	Run(ctx context.Context)
	CommitMessages(msgIDs ...int64) error
}

type MessageHandler func(ctx context.Context, msg *Message) error
type ErrorMessageHandler func(ctx context.Context, msg *Message, lastErr error) error

type ConsumerOptions struct {
	// AuthType is the type of authentication.
	// Default: raw.
	AuthType   auth.AuthType
	AssumeRole string

	// ErrHandler is the handler for error message.
	// Default: nil
	ErrHandler ErrorMessageHandler
	// OrderdMode is the order mode of consuming messages. If true, consume messages with the same key
	// in order, otherwise consume all messages in random order.
	// Default: false
	OrderedMode bool
	// Retryer is the retryer for retrying failed messages.
	// If return a NotRetryErr, the message will be dropped.
	// Default: FastSlowRetryer.
	Retryer Retryer

	// ParalSize is the number of parallel workers to consume messages.
	// Default: 5
	ParalSize int
	// CacheSize is the size of cache queue.
	// Default: 100
	CacheSize int
	// FetchMinBytes is the minimum number of bytes to fetch in one batch from brokers.
	// Default: 1
	FetchMinBytes int
	// FetchMaxBytes is the maximum number of bytes to fetch in one batch from brokers.
	// Default: 1MB
	FetchMaxBytes int
	// FetchMaxWait is the maximum time to wait in one batch from brokers.
	// Default: 10s
	FetchMaxWait time.Duration
	// StartOffset is the begining offset when no commited offset in partition. If
	// non-zero, it must be set to one of kafka.FirstOffset(-2) or kafka.LastOffset(-1).
	// Default: kafka.LastOffset
	StartOffset int64

	// AutoCommit enabled the auto commit of offset. If true, auto commit offset after message handled, else you
	// should call CommitMessages manually.
	// Default: true
	AutoCommit *bool
	// CommitInterval is the interval to commit offset.
	// Default: 2s
	CommitInterval time.Duration
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
		o.FetchMinBytes = 1 // 1
	}
	if o.FetchMaxBytes == 0 {
		o.FetchMaxBytes = 1024 * 1024 // 1MB
	}
	if o.FetchMaxWait == 0 {
		o.FetchMaxWait = 10 * time.Second
	}
	if o.StartOffset == 0 {
		o.StartOffset = kafka.LastOffset
	}
	if o.StartOffset != kafka.FirstOffset && o.StartOffset != kafka.LastOffset {
		return errors.New("invalid start offset of consumer")
	}
	if o.Retryer == nil {
		o.Retryer = NewFastSlowRetryer(msgFastRetryLimit, msgFastRetryInterval, msgSlowRetryInterval)
	}
	if o.AutoCommit == nil {
		o.AutoCommit = utils.Ptr(true)
	}
	if o.CommitInterval == 0 {
		o.CommitInterval = 2 * time.Second
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
	msgRetryer   Retryer

	offsetMgr  *offsetManager
	autoCommit bool
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
		options:    opts,
		brokers:    brokers,
		topic:      topic,
		groupID:    groupID,
		handler:    handler,
		msgRetryer: opts.Retryer,

		running: &atomic.Bool{},
	}

	if opts.AuthType == auth.AuthTypeRaw {
		c.author = auth.NewRawAuthenticator()
	}

	c.reader = kafka.NewReader(kafka.ReaderConfig{
		Dialer:      c.author.GetDialer(opts.AssumeRole),
		Brokers:     strings.Split(brokers, ","),
		Topic:       topic,
		GroupID:     groupID,
		MinBytes:    c.options.FetchMinBytes,
		MaxBytes:    c.options.FetchMaxBytes,
		MaxWait:     c.options.FetchMaxWait,
		StartOffset: c.options.StartOffset,
	})
	if opts.OrderedMode {
		c.consumeQueue = newOrderedQueue(opts.CacheSize, opts.ParalSize, c.handleMessage)
	} else {
		c.consumeQueue = newUnorderedQueue(opts.CacheSize, opts.ParalSize, c.handleMessage)
	}

	if opts.AutoCommit != nil && *(opts.AutoCommit) {
		c.autoCommit = true
	}
	c.offsetMgr = newOffsetManager(c.reader, opts.CommitInterval)

	return c, nil
}

func (c *consumer) Run(ctx context.Context) {
	c.running.Store(true)

	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		c.consume(ctx)
	}()
	go func() {
		defer wg.Done()
		c.consumeQueue.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		c.offsetMgr.Run(ctx)
	}()

	<-ctx.Done()
	c.running.Store(false)
	wg.Wait()
	c.close()
}

func (c *consumer) close() {
	if err := c.reader.Close(); err != nil {
		log.Warnw("Failed to close kafka reader", "error", err)
	}
	log.Infow("Consumer stopped", "topic", c.topic)
}

func (c *consumer) consume(ctx context.Context) {
	for c.running.Load() {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if c.running.Load() {
				log.Errorw("Error fetch kafka message", "topic", c.topic, "error", err)
				time.Sleep(errorRetryInterval)
			}
			continue
		}
		c.enqueueMessage(ctx, &msg)
	}
}

func (c *consumer) enqueueMessage(ctx context.Context, msg *kafka.Message) {
	seqID := c.offsetMgr.addMessage(msg)
	c.consumeQueue.Add(ctx, seqID, msg)
}

func (c *consumer) handleMessage(ctx context.Context, msgID int64, m *kafka.Message) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorw("Panic", "error", r)
		}
	}()

	msg := &Message{
		ID:      msgID,
		Key:     string(m.Key),
		Value:   m.Value,
		Headers: m.Headers,
	}

	err := c.msgRetryer.Execute(func() error {
		return c.handler(ctx, msg)
	})
	if err != nil {
		log.Errorw("Error handle message from kafka", "error", err, "key", msg.Key,
			"body", string(msg.Value))
		if !errors.Is(err, utils.NotRetryErr) && c.options.ErrHandler != nil {
			_ = c.options.ErrHandler(ctx, msg, err) //nolint
		}
	}

	// Auto commit offsets
	if c.autoCommit {
		c.offsetMgr.finish(msgID)
	}
}

func (c *consumer) CommitMessages(msgIDs ...int64) error {
	if c.autoCommit {
		return errors.New("auto commit is enabled, can not call commit manually")
	}
	c.offsetMgr.finish(msgIDs...)
	return nil
}
