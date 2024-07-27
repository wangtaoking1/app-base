// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wangtaoking1/app-base/utils"

	"github.com/panjf2000/ants/v2"
	"github.com/segmentio/kafka-go"

	"github.com/wangtaoking1/app-base/kafka/auth"
	"github.com/wangtaoking1/app-base/log"
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
	// BatchSize is the number of messages to consume in one batch.
	// Default: 50
	BatchSize int
	// CacheSize is the size of cache queue.
	// Default: 200
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
	if o.BatchSize == 0 {
		o.BatchSize = 50
	}
	if o.CacheSize == 0 {
		o.CacheSize = 200
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

	author  auth.Authenticator
	running *atomic.Bool
	pool    *ants.Pool
	q       chan kafka.Message
	random  *rand.Rand
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
		random:  rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec
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
	var err error
	c.pool, err = ants.NewPool(c.options.ParalSize)
	if err != nil {
		return nil, err
	}
	c.q = make(chan kafka.Message, c.options.CacheSize)

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
		c.dispatch(ctx)
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
	if err := c.pool.ReleaseTimeout(3 * time.Second); err != nil {
		log.Warn("Failed to release worker pool for consumer", "error", err)
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
		c.q <- msg
	}
}

func (c *consumer) dispatch(ctx context.Context) {
	messages := make([]kafka.Message, 0)

	maxWait, batchSize := c.options.FetchMaxWait, c.options.BatchSize
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	for {
		timer.Reset(maxWait)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if len(messages) > 0 {
				c.handleMessages(ctx, messages)
				messages = messages[:0]
			}
		case msg := <-c.q:
			messages = append(messages, msg)
			if len(messages) >= batchSize {
				c.handleMessages(ctx, messages)
				messages = messages[:0]
			}

			// Consume all messages in cache queue directly.
		loop:
			for {
				select {
				case msg = <-c.q:
					messages = append(messages, msg)
					if len(messages) >= batchSize {
						c.handleMessages(ctx, messages)
						messages = messages[:0]
					}
				default:
					break loop
				}
			}
		}
	}
}

func (c *consumer) handleMessages(ctx context.Context, messages []kafka.Message) {
	if len(messages) == 0 {
		return
	}
	log.Debug("Handle messages", "topic", c.topic, "count", len(messages))

	if c.options.OrderedMode {
		c.handleMessagesInOrdered(ctx, messages)
	} else {
		c.handleMessagesInUnordered(ctx, messages)
	}

	c.commitOffset(ctx, messages)
}

func (c *consumer) handleMessagesInOrdered(ctx context.Context, messages []kafka.Message) {
	totalPart := utils.Min(len(messages), 2*c.options.ParalSize)
	messageParts := c.splitMsgsByKey(messages, totalPart)
	wg := &sync.WaitGroup{}
	// Parallel handle messages.
	// Control the order of messages with the same key.
	for i := range messageParts {
		ms := messageParts[i]
		if len(ms) == 0 {
			continue
		}
		wg.Add(1)
		_ = c.pool.Submit(func() {
			defer func() {
				wg.Done()
			}()
			for _, m := range ms {
				c.doHandle(ctx, m)
			}
		})
	}
	wg.Wait()
}

func (c *consumer) handleMessagesInUnordered(ctx context.Context, messages []kafka.Message) {
	wg := &sync.WaitGroup{}
	// Parallel handle messages.
	for i := range messages {
		m := messages[i]
		wg.Add(1)
		_ = c.pool.Submit(func() {
			defer func() {
				wg.Done()
			}()
			c.doHandle(ctx, m)
		})
	}
	wg.Wait()
}

func (c *consumer) doHandle(ctx context.Context, m kafka.Message) {
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
		if c.options.ErrHandler != nil {
			_ = c.options.ErrHandler(ctx, msg, err)
		}
		return
	}
}

func (c *consumer) commitOffset(ctx context.Context, messages []kafka.Message) {
	if c.reader == nil {
		return
	}
	if err := c.reader.CommitMessages(ctx, messages...); err != nil {
		log.Error("Error commit consumer offset to kafka", "topic", c.topic, "error", err.Error())
	}
}

func (c *consumer) splitMsgsByKey(messages []kafka.Message, cnt int) [][]kafka.Message {
	results := make([][]kafka.Message, cnt)
	for i := range messages {
		msg := messages[i]
		idx := c.hashByKey(string(msg.Key), cnt)
		results[idx] = append(results[idx], msg)
	}
	return results
}

func (c *consumer) hashByKey(key string, cnt int) int {
	if key == "" {
		return c.random.Intn(cnt)
	}
	return int(utils.StringHash(key) % uint32(cnt))
}
