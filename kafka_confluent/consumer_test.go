// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka_confluent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wangtaoking1/app-base/utils"
)

func TestConsumerOptions_SetDefaults(t *testing.T) {
	t.Run("empty options get defaults", func(t *testing.T) {
		opts := &ConsumerOptions{}
		require.NoError(t, opts.SetDefaults())

		assert.Equal(t, AuthTypeRaw, opts.AuthType)
		assert.Equal(t, 5, opts.ParalSize)
		assert.Equal(t, 100, opts.CacheSize)
		assert.Equal(t, 1, opts.FetchMinBytes)
		assert.Equal(t, 1024*1024, opts.FetchMaxBytes)
		assert.Equal(t, 10*time.Second, opts.FetchMaxWait)
		assert.Equal(t, OffsetLatest, opts.StartOffset)
		assert.Equal(t, 2*time.Second, opts.CommitInterval)
		assert.Equal(t, 10*time.Second, opts.SessionTimeout)
		assert.Equal(t, 3*time.Second, opts.HeartbeatInterval)
		require.NotNil(t, opts.AutoCommit)
		assert.True(t, *opts.AutoCommit)
		assert.NotNil(t, opts.Retryer)
	})

	t.Run("explicit values are preserved", func(t *testing.T) {
		opts := &ConsumerOptions{
			ParalSize:   10,
			CacheSize:   200,
			StartOffset: OffsetEarliest,
			AutoCommit:  utils.Ptr(false),
		}
		require.NoError(t, opts.SetDefaults())

		assert.Equal(t, 10, opts.ParalSize)
		assert.Equal(t, 200, opts.CacheSize)
		assert.Equal(t, OffsetEarliest, opts.StartOffset)
		require.NotNil(t, opts.AutoCommit)
		assert.False(t, *opts.AutoCommit)
	})

	t.Run("invalid StartOffset returns error", func(t *testing.T) {
		opts := &ConsumerOptions{StartOffset: 99}
		err := opts.SetDefaults()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "StartOffset")
	})
}

func TestConsumerOptions_ToConfigMap(t *testing.T) {
	t.Run("raw auth produces correct config", func(t *testing.T) {
		opts := &ConsumerOptions{
			StartOffset:       OffsetEarliest,
			SessionTimeout:    30 * time.Second,
			HeartbeatInterval: 5 * time.Second,
			FetchMinBytes:     512,
			FetchMaxBytes:     2 * 1024 * 1024,
			FetchMaxWait:      5 * time.Second,
		}
		require.NoError(t, opts.SetDefaults())

		cfg := opts.toConfigMap("b:9092", "test-group")

		assert.Equal(t, "b:9092", cfg["bootstrap.servers"])
		assert.Equal(t, "test-group", cfg["group.id"])
		assert.Equal(t, false, cfg["enable.auto.commit"],
			"offset management is always manual")
		assert.Equal(t, "earliest", cfg["auto.offset.reset"])
		assert.Equal(t, int(30000), cfg["session.timeout.ms"])
		assert.Equal(t, int(5000), cfg["heartbeat.interval.ms"])
		assert.Equal(t, 512, cfg["fetch.min.bytes"])
		assert.Equal(t, 2*1024*1024, cfg["fetch.max.bytes"])
		assert.Equal(t, int(5000), cfg["fetch.wait.max.ms"])
		assert.Nil(t, cfg["security.protocol"])
	})

	t.Run("latest offset maps to 'latest'", func(t *testing.T) {
		opts := &ConsumerOptions{}
		require.NoError(t, opts.SetDefaults())
		cfg := opts.toConfigMap("b:9092", "g")
		assert.Equal(t, "latest", cfg["auto.offset.reset"])
	})

	t.Run("SASL auth injects credentials", func(t *testing.T) {
		opts := &ConsumerOptions{
			AuthType:    AuthTypeSASL,
			SASLOptions: &SASLOptions{Username: "bob", Password: "pw"},
		}
		require.NoError(t, opts.SetDefaults())
		cfg := opts.toConfigMap("b:9093", "g")

		assert.Equal(t, "SASL_PLAINTEXT", cfg["security.protocol"])
		assert.Equal(t, "PLAIN", cfg["sasl.mechanism"])
		assert.Equal(t, "bob", cfg["sasl.username"])
		assert.Equal(t, "pw", cfg["sasl.password"])
	})
}

func TestStartOffset_ToConfigString(t *testing.T) {
	assert.Equal(t, "earliest", OffsetEarliest.toConfigString())
	assert.Equal(t, "latest", OffsetLatest.toConfigString())
	assert.Equal(t, "latest", StartOffset(0).toConfigString(), "unknown value falls back to latest")
}

func TestFromConfluentMessage(t *testing.T) {
	topic := "test-topic"

	ckMsg := buildMockConfluentMessage(topic, []byte("my-key"), []byte("my-value"), 2, 42)
	raw := fromConfluentMessage(ckMsg)

	assert.Equal(t, []byte("my-key"), raw.key)
	assert.Equal(t, []byte("my-value"), raw.value)
	assert.Equal(t, "test-topic", raw.topic)
	assert.Equal(t, int32(2), raw.partition)
	assert.Equal(t, int64(42), raw.offset)
}

func TestToConfluentMessage(t *testing.T) {
	msg := &Message{
		Key:   "k1",
		Value: []byte("hello"),
		Headers: []Header{
			{Key: "h1", Value: []byte("v1")},
		},
	}
	km := toConfluentMessage("topic-x", msg)

	assert.Equal(t, "topic-x", *km.TopicPartition.Topic)
	assert.Equal(t, []byte("k1"), km.Key)
	assert.Equal(t, []byte("hello"), km.Value)
	require.Len(t, km.Headers, 1)
	assert.Equal(t, "h1", km.Headers[0].Key)
	assert.Equal(t, []byte("v1"), km.Headers[0].Value)
}

func TestToConfluentMessage_NoHeaders(t *testing.T) {
	msg := &Message{Key: "k", Value: []byte("v")}
	km := toConfluentMessage("t", msg)
	assert.Nil(t, km.Headers, "nil headers must not be allocated when empty")
}
