// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka_confluent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProducerOptions_SetDefaults(t *testing.T) {
	t.Run("empty options get defaults", func(t *testing.T) {
		opts := &ProducerOptions{}
		opts.SetDefaults()

		assert.Equal(t, AuthTypeRaw, opts.AuthType)
		assert.Equal(t, CompressionGzip, opts.Compression)
		assert.False(t, opts.Async)
	})

	t.Run("existing values are preserved", func(t *testing.T) {
		opts := &ProducerOptions{
			AuthType:    AuthTypeSASL,
			Compression: CompressionSnappy,
			Async:       true,
		}
		opts.SetDefaults()

		assert.Equal(t, AuthTypeSASL, opts.AuthType)
		assert.Equal(t, CompressionSnappy, opts.Compression)
		assert.True(t, opts.Async)
	})
}

func TestProducerOptions_Validate(t *testing.T) {
	t.Run("SASL without options returns error", func(t *testing.T) {
		opts := &ProducerOptions{AuthType: AuthTypeSASL}
		opts.SetDefaults()
		assert.Error(t, opts.validate())
	})

	t.Run("SASL with options is valid", func(t *testing.T) {
		opts := &ProducerOptions{
			AuthType:    AuthTypeSASL,
			SASLOptions: &SASLOptions{Username: "u", Password: "p"},
		}
		opts.SetDefaults()
		assert.NoError(t, opts.validate())
	})

	t.Run("raw auth is always valid", func(t *testing.T) {
		opts := &ProducerOptions{}
		opts.SetDefaults()
		assert.NoError(t, opts.validate())
	})
}

func TestProducerOptions_ToConfigMap(t *testing.T) {
	t.Run("raw auth produces minimal config", func(t *testing.T) {
		opts := &ProducerOptions{
			AuthType:    AuthTypeRaw,
			Compression: CompressionGzip,
			RequireAcks: RequireAll,
		}
		opts.SetDefaults()
		cfg := opts.toConfigMap("broker1:9092,broker2:9092")

		assert.Equal(t, "broker1:9092,broker2:9092", cfg["bootstrap.servers"])
		assert.Equal(t, "-1", cfg["acks"])
		assert.Equal(t, "gzip", cfg["compression.type"])
		assert.Nil(t, cfg["security.protocol"], "raw auth must not set security.protocol")
	})

	t.Run("SASL auth injects credentials", func(t *testing.T) {
		opts := &ProducerOptions{
			AuthType:    AuthTypeSASL,
			SASLOptions: &SASLOptions{Username: "alice", Password: "secret"},
			Compression: CompressionNone,
		}
		opts.SetDefaults()
		cfg := opts.toConfigMap("broker:9093")

		assert.Equal(t, "SASL_PLAINTEXT", cfg["security.protocol"])
		assert.Equal(t, "PLAIN", cfg["sasl.mechanism"])
		assert.Equal(t, "alice", cfg["sasl.username"])
		assert.Equal(t, "secret", cfg["sasl.password"])
	})

	t.Run("optional batch settings only set when non-zero", func(t *testing.T) {
		opts := &ProducerOptions{
			BatchSize:    500,
			BatchBytes:   1 << 20,
			BatchTimeout: 100 * time.Millisecond,
		}
		opts.SetDefaults()
		cfg := opts.toConfigMap("b:9092")

		assert.Equal(t, 500, cfg["batch.num.messages"])
		assert.Equal(t, 1<<20, cfg["batch.size"])
		assert.Equal(t, int(100), cfg["linger.ms"])
	})

	t.Run("zero batch settings are omitted", func(t *testing.T) {
		opts := &ProducerOptions{}
		opts.SetDefaults()
		cfg := opts.toConfigMap("b:9092")

		assert.Nil(t, cfg["batch.num.messages"])
		assert.Nil(t, cfg["batch.size"])
		assert.Nil(t, cfg["linger.ms"])
	})
}

func TestNewProducerWithOptions_ValidationError(t *testing.T) {
	_, err := NewProducerWithOptions("broker:9092", "topic", &ProducerOptions{
		AuthType: AuthTypeSASL,
		// Missing SASLOptions intentionally
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SASL options")
}

func TestRequiredAcksConstants(t *testing.T) {
	assert.Equal(t, RequiredAcks(0), RequireNone)
	assert.Equal(t, RequiredAcks(1), RequireOne)
	assert.Equal(t, RequiredAcks(-1), RequireAll)
}
