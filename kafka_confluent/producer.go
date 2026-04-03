// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka_confluent

import (
	"context"
	"strconv"
	"strings"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pkg/errors"
	"github.com/wangtaoking1/app-base/log"
)

// Producer sends messages to kafka topics.
type Producer interface {
	// SendMessage sends messages to the topic configured at creation time.
	SendMessage(ctx context.Context, msgs ...Message) error
	// SendMessageWithTopic sends messages to the given topic.
	// Requires the producer to have been created without a default topic.
	SendMessageWithTopic(ctx context.Context, topic string, msgs ...Message) error
	// Close flushes pending messages and closes the underlying producer.
	Close()
}

// ProducerOptions configures a Producer. All types are package-local; no
// confluent-kafka-go types are exposed to callers.
type ProducerOptions struct {
	// AuthType selects the authentication mechanism. Default: AuthTypeRaw.
	AuthType AuthType
	// SASLOptions provides credentials when AuthType == AuthTypeSASL.
	SASLOptions *SASLOptions

	// RequireAcks controls broker acknowledgement. Default: RequireNone.
	RequireAcks RequiredAcks

	// Async enables fire-and-forget message delivery.
	// Delivery errors are logged but not returned from SendMessage.
	// Default: false (synchronous).
	Async bool

	// BatchSize is the maximum number of messages per batch.
	// Maps to confluent's batch.num.messages. Default: 0 (confluent default).
	BatchSize int

	// BatchBytes is the maximum batch size in bytes.
	// Maps to confluent's batch.size. Default: 0 (confluent default = 1MB).
	BatchBytes int

	// BatchTimeout is the maximum time to wait before flushing an incomplete batch.
	// Maps to confluent's linger.ms. Default: 0 (confluent default = 5ms).
	BatchTimeout time.Duration

	// Compression selects the message compression codec. Default: CompressionGzip.
	Compression CompressionType
}

func (o *ProducerOptions) SetDefaults() {
	if o.AuthType == "" {
		o.AuthType = AuthTypeRaw
	}
	if o.Compression == "" {
		o.Compression = CompressionGzip
	}
}

func (o *ProducerOptions) validate() error {
	if o.AuthType == AuthTypeSASL && o.SASLOptions == nil {
		return errors.New("SASL options must be specified for SASL authentication")
	}
	return nil
}

// toConfigMap builds the confluent ConfigMap from the options and broker list.
func (o *ProducerOptions) toConfigMap(brokers string) ckafka.ConfigMap {
	cfg := ckafka.ConfigMap{
		"bootstrap.servers": brokers,
		"acks":              strconv.Itoa(int(o.RequireAcks)),
		"compression.type":  string(o.Compression),
	}
	if o.BatchSize > 0 {
		cfg["batch.num.messages"] = o.BatchSize
	}
	if o.BatchBytes > 0 {
		cfg["batch.size"] = o.BatchBytes
	}
	if o.BatchTimeout > 0 {
		cfg["linger.ms"] = int(o.BatchTimeout.Milliseconds())
	}
	if o.AuthType == AuthTypeSASL && o.SASLOptions != nil {
		cfg["security.protocol"] = "SASL_PLAINTEXT"
		cfg["sasl.mechanism"] = "PLAIN"
		cfg["sasl.username"] = o.SASLOptions.Username
		cfg["sasl.password"] = o.SASLOptions.Password
	}
	return cfg
}

type producer struct {
	topic     string
	options   *ProducerOptions
	kproducer *ckafka.Producer
}

// NewProducer creates a producer that sends to a fixed topic.
func NewProducer(brokers, topic string) (Producer, error) {
	return NewProducerWithOptions(brokers, topic, &ProducerOptions{})
}

// NewProducerWithOptions creates a producer with the given options.
// Pass an empty topic to use SendMessageWithTopic for per-call routing.
func NewProducerWithOptions(brokers, topic string, opts *ProducerOptions) (Producer, error) {
	opts.SetDefaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}

	cfg := opts.toConfigMap(strings.TrimSpace(brokers))
	kp, err := ckafka.NewProducer(&cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create kafka producer")
	}

	p := &producer{
		topic:     topic,
		options:   opts,
		kproducer: kp,
	}

	if opts.Async {
		go p.drainEvents()
	}
	return p, nil
}

// drainEvents consumes the confluent producer's event channel in the background
// to prevent it from blocking, and logs delivery errors.
func (p *producer) drainEvents() {
	for e := range p.kproducer.Events() {
		if msg, ok := e.(*ckafka.Message); ok {
			if msg.TopicPartition.Error != nil {
				log.Errorw("Async kafka delivery error", "error", msg.TopicPartition.Error,
					"topic", *msg.TopicPartition.Topic)
			}
		}
	}
}

func (p *producer) Close() {
	if p.kproducer == nil {
		return
	}
	p.kproducer.Flush(5000)
	p.kproducer.Close()
}

func (p *producer) SendMessage(ctx context.Context, msgs ...Message) error {
	if p.topic == "" {
		return errors.New("no topic specified in Producer; use SendMessageWithTopic instead")
	}
	return p.send(ctx, p.topic, msgs)
}

func (p *producer) SendMessageWithTopic(ctx context.Context, topic string, msgs ...Message) error {
	if p.topic != "" {
		return errors.New("producer already has a fixed topic; use SendMessage instead")
	}
	return p.send(ctx, topic, msgs)
}

func (p *producer) send(ctx context.Context, topic string, msgs []Message) error {
	if p.options.Async {
		return p.sendAsync(topic, msgs)
	}
	return p.sendSync(ctx, topic, msgs)
}

// sendSync produces messages and waits for delivery confirmation.
func (p *producer) sendSync(ctx context.Context, topic string, msgs []Message) error {
	deliveryChan := make(chan ckafka.Event, len(msgs))
	for i := range msgs {
		km := toConfluentMessage(topic, &msgs[i])
		if err := p.kproducer.Produce(km, deliveryChan); err != nil {
			return errors.Wrap(err, "kafka produce error")
		}
	}

	for range len(msgs) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-deliveryChan:
			m, ok := e.(*ckafka.Message)
			if !ok {
				continue
			}
			if m.TopicPartition.Error != nil {
				return m.TopicPartition.Error
			}
		}
	}
	return nil
}

// sendAsync produces messages without waiting for delivery confirmation.
func (p *producer) sendAsync(topic string, msgs []Message) error {
	for i := range msgs {
		km := toConfluentMessage(topic, &msgs[i])
		// nil delivery channel routes events to kproducer.Events(), drained by drainEvents().
		if err := p.kproducer.Produce(km, nil); err != nil {
			return errors.Wrap(err, "kafka produce error")
		}
	}
	return nil
}

func toConfluentMessage(topic string, msg *Message) *ckafka.Message {
	km := &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{
			Topic:     &topic,
			Partition: ckafka.PartitionAny,
		},
		Key:   []byte(msg.Key),
		Value: msg.Value,
	}
	if len(msg.Headers) > 0 {
		km.Headers = make([]ckafka.Header, len(msg.Headers))
		for i, h := range msg.Headers {
			km.Headers[i] = ckafka.Header{Key: h.Key, Value: h.Value}
		}
	}
	return km
}
