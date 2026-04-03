// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka_confluent

import (
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// buildMockConfluentMessage constructs a confluent kafka.Message for use in tests.
func buildMockConfluentMessage(topic string, key, value []byte, partition int32, offset int64) *ckafka.Message {
	t := topic
	return &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{
			Topic:     &t,
			Partition: partition,
			Offset:    ckafka.Offset(offset),
		},
		Key:       key,
		Value:     value,
		Timestamp: time.Now(),
	}
}
