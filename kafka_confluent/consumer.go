// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka_confluent

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pkg/errors"
	"github.com/wangtaoking1/app-base/log"
	"github.com/wangtaoking1/app-base/utils"
)

const (
	// errorRetryInterval is the delay before retrying after a broker error.
	errorRetryInterval = 5 * time.Second

	// readTimeout is how long ReadMessage blocks before checking running state.
	readTimeout = time.Second

	msgFastRetryLimit    = 3
	msgFastRetryInterval = 10 * time.Millisecond
	msgSlowRetryInterval = 30 * time.Second
)

// Consumer reads messages from a kafka topic and dispatches them to a handler.
type Consumer interface {
	// Run starts the consumer loop. It blocks until ctx is cancelled.
	Run(ctx context.Context)
	// CommitMessages manually commits offsets for the given message IDs.
	// Only valid when AutoCommit is false.
	CommitMessages(msgIDs ...int64) error
}

// ConsumerOptions configures a Consumer.
type ConsumerOptions struct {
	// AuthType selects the authentication mechanism. Default: AuthTypeRaw.
	AuthType AuthType
	// SASLOptions provides credentials when AuthType == AuthTypeSASL.
	SASLOptions *SASLOptions

	// ErrHandler is called when a message fails all retries.
	ErrHandler ErrorMessageHandler

	// OrderedMode ensures messages with the same key are processed in order.
	// Default: false (parallel, unordered).
	OrderedMode bool

	// Retryer controls retry behaviour for failed messages.
	// Default: FastSlowRetryer.
	Retryer Retryer

	// ParalSize is the number of concurrent message handlers. Default: 5.
	ParalSize int
	// CacheSize is the per-worker channel buffer size. Default: 100.
	CacheSize int

	// FetchMinBytes is the minimum fetch size in bytes. Default: 1.
	FetchMinBytes int
	// FetchMaxBytes is the maximum fetch size in bytes. Default: 1MB.
	FetchMaxBytes int
	// FetchMaxWait is the maximum time to wait for FetchMinBytes. Default: 10s.
	FetchMaxWait time.Duration

	// StartOffset controls where to start reading when no committed offset exists.
	// Default: OffsetLatest.
	StartOffset StartOffset

	// SessionTimeout is the consumer group session timeout. Default: 10s.
	SessionTimeout time.Duration
	// HeartbeatInterval is the consumer group heartbeat interval. Default: 3s.
	HeartbeatInterval time.Duration

	// AutoCommit enables automatic offset commit after each handled message.
	// Default: true.
	AutoCommit *bool
	// CommitInterval controls how often the offset manager flushes. Default: 2s.
	CommitInterval time.Duration
}

func (o *ConsumerOptions) SetDefaults() error {
	if o.AuthType == "" {
		o.AuthType = AuthTypeRaw
	}
	if o.ParalSize == 0 {
		o.ParalSize = 5
	}
	if o.CacheSize == 0 {
		o.CacheSize = 100
	}
	if o.FetchMinBytes == 0 {
		o.FetchMinBytes = 1
	}
	if o.FetchMaxBytes == 0 {
		o.FetchMaxBytes = 1024 * 1024
	}
	if o.FetchMaxWait == 0 {
		o.FetchMaxWait = 10 * time.Second
	}
	if o.StartOffset == 0 {
		o.StartOffset = OffsetLatest
	}
	if o.StartOffset != OffsetEarliest && o.StartOffset != OffsetLatest {
		return errors.New("invalid StartOffset: must be OffsetEarliest or OffsetLatest")
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
	if o.SessionTimeout == 0 {
		o.SessionTimeout = 10 * time.Second
	}
	if o.HeartbeatInterval == 0 {
		o.HeartbeatInterval = 3 * time.Second
	}
	return nil
}

// toConfigMap builds the confluent ConfigMap from the options.
func (o *ConsumerOptions) toConfigMap(brokers, groupID string) ckafka.ConfigMap {
	cfg := ckafka.ConfigMap{
		"bootstrap.servers":       brokers,
		"group.id":                groupID,
		"enable.auto.commit":      false, // managed manually via offsetManager
		"auto.offset.reset":       o.StartOffset.toConfigString(),
		"session.timeout.ms":      int(o.SessionTimeout.Milliseconds()),
		"heartbeat.interval.ms":   int(o.HeartbeatInterval.Milliseconds()),
		"fetch.min.bytes":         o.FetchMinBytes,
		"fetch.max.bytes":         o.FetchMaxBytes,
		"fetch.wait.max.ms":       int(o.FetchMaxWait.Milliseconds()),
	}
	if o.AuthType == AuthTypeSASL && o.SASLOptions != nil {
		cfg["security.protocol"] = "SASL_PLAINTEXT"
		cfg["sasl.mechanism"] = "PLAIN"
		cfg["sasl.username"] = o.SASLOptions.Username
		cfg["sasl.password"] = o.SASLOptions.Password
	}
	return cfg
}

type consumer struct {
	options    *ConsumerOptions
	brokers    string
	topic      string
	groupID    string
	handler    MessageHandler
	kreader    *ckafka.Consumer
	running    *atomic.Bool
	cqueue     ConsumeQueue
	msgRetryer Retryer
	offsetMgr  *offsetManager
	autoCommit bool
}

// NewConsumer creates a consumer with default options.
func NewConsumer(brokers, topic, groupID string, handler MessageHandler) (Consumer, error) {
	return NewConsumerWithOptions(brokers, topic, groupID, handler, &ConsumerOptions{})
}

// NewConsumerWithOptions creates a consumer with the given options.
func NewConsumerWithOptions(
	brokers, topic, groupID string,
	handler MessageHandler,
	opts *ConsumerOptions,
) (Consumer, error) {
	if err := opts.SetDefaults(); err != nil {
		return nil, err
	}

	cfg := opts.toConfigMap(strings.TrimSpace(brokers), groupID)
	kr, err := ckafka.NewConsumer(&cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create kafka consumer")
	}
	if err := kr.Subscribe(strings.TrimSpace(topic), nil); err != nil {
		_ = kr.Close()
		return nil, errors.Wrap(err, "failed to subscribe to topic")
	}

	c := &consumer{
		options:    opts,
		brokers:    brokers,
		topic:      topic,
		groupID:    groupID,
		handler:    handler,
		kreader:    kr,
		running:    &atomic.Bool{},
		msgRetryer: opts.Retryer,
		autoCommit: opts.AutoCommit != nil && *opts.AutoCommit,
	}

	if opts.OrderedMode {
		c.cqueue = newOrderedQueue(opts.CacheSize, opts.ParalSize, c.handleMessage)
	} else {
		c.cqueue = newUnorderedQueue(opts.CacheSize, opts.ParalSize, c.handleMessage)
	}

	c.offsetMgr = newOffsetManager(opts.CommitInterval, c.commitOffset)
	return c, nil
}

// Run starts the consumer loop and blocks until ctx is cancelled.
func (c *consumer) Run(ctx context.Context) {
	c.running.Store(true)

	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() { defer wg.Done(); c.consume(ctx) }()
	go func() { defer wg.Done(); c.cqueue.Run(ctx) }()
	go func() { defer wg.Done(); c.offsetMgr.Run(ctx) }()

	<-ctx.Done()
	c.running.Store(false)
	wg.Wait()
	c.close()
}

func (c *consumer) close() {
	if err := c.kreader.Close(); err != nil {
		log.Warnw("Failed to close kafka consumer", "error", err)
	}
	log.Infow("Consumer stopped", "topic", c.topic)
}

func (c *consumer) consume(ctx context.Context) {
	for c.running.Load() {
		c.consumeLoop(ctx)
	}
}

// consumeLoop runs one iteration of the consume loop with panic recovery.
// On panic, it logs and returns so consume() can restart it.
func (c *consumer) consumeLoop(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorw("Panic in consume loop, will restart", "error", r)
		}
	}()

	for c.running.Load() {
		msg, err := c.kreader.ReadMessage(readTimeout)
		if err != nil {
			kerr, ok := err.(ckafka.Error)
			if ok && kerr.Code() == ckafka.ErrTimedOut {
				continue // normal poll timeout, check running
			}
			if !c.running.Load() {
				return
			}
			log.Errorw("Error reading kafka message", "topic", c.topic, "error", err)
			time.Sleep(errorRetryInterval)
			continue
		}
		c.enqueueMessage(ctx, msg)
	}
}

func (c *consumer) enqueueMessage(ctx context.Context, msg *ckafka.Message) {
	raw := fromConfluentMessage(msg)
	seqID := c.offsetMgr.addMessage(raw)
	c.cqueue.Add(ctx, seqID, raw)
}

func (c *consumer) handleMessage(ctx context.Context, msgID int64, raw *rawMessage) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorw("Panic in message handler", "error", r)
		}
	}()

	msg := &Message{
		ID:      msgID,
		Key:     string(raw.key),
		Value:   raw.value,
		Headers: raw.headers,
		Time:    raw.timestamp,
	}

	err := c.msgRetryer.Execute(ctx, func() error {
		return c.handler(ctx, msg)
	})
	if err != nil {
		log.Errorw("Error handling kafka message", "error", err, "key", msg.Key)
		if !errors.Is(err, utils.NotRetryErr) && c.options.ErrHandler != nil {
			_ = c.options.ErrHandler(ctx, msg, err) //nolint
		}
	}

	if c.autoCommit {
		c.offsetMgr.finish(msgID)
	}
}

func (c *consumer) CommitMessages(msgIDs ...int64) error {
	if c.autoCommit {
		return errors.New("auto commit is enabled; call CommitMessages only when AutoCommit is false")
	}
	c.offsetMgr.finish(msgIDs...)
	return nil
}

// commitOffset is the offsetCommitter injected into the offsetManager.
// It converts rawMessages to confluent TopicPartition commits.
func (c *consumer) commitOffset(ctx context.Context, msgs []*rawMessage) error {
	offsets := commitOffsetsByPartition(msgs)
	tps := make([]ckafka.TopicPartition, len(offsets))
	for i, po := range offsets {
		topic := po.Topic
		tps[i] = ckafka.TopicPartition{
			Topic:     &topic,
			Partition: po.Partition,
			Offset:    ckafka.Offset(po.Offset),
		}
	}
	return retryCommit(ctx, func() error {
		_, err := c.kreader.CommitOffsets(tps)
		return err
	})
}

// fromConfluentMessage converts a confluent kafka.Message to an internal rawMessage.
func fromConfluentMessage(msg *ckafka.Message) *rawMessage {
	raw := &rawMessage{
		key:       msg.Key,
		value:     msg.Value,
		timestamp: msg.Timestamp,
		partition: msg.TopicPartition.Partition,
		offset:    int64(msg.TopicPartition.Offset),
	}
	if msg.TopicPartition.Topic != nil {
		raw.topic = *msg.TopicPartition.Topic
	}
	if len(msg.Headers) > 0 {
		raw.headers = make([]Header, len(msg.Headers))
		for i, h := range msg.Headers {
			raw.headers[i] = Header{Key: h.Key, Value: h.Value}
		}
	}
	return raw
}
