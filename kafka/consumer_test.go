// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"

	"github.com/wangtaoking1/app-base/log"
)

func Test_Paral_Simple(t *testing.T) {
	log.Init(nil)
	c := &consumer{
		options: &ConsumerOptions{
			ParalSize:   1,
			OrderedMode: true,
		},
		random: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	c.pool, _ = ants.NewPool(c.options.ParalSize)

	var results []string
	var mtx sync.Mutex
	c.handler = func(ctx context.Context, msg *Message) error {
		mtx.Lock()
		defer mtx.Unlock()

		results = append(results, string(msg.Value))
		return nil
	}

	msgs := []kafka.Message{
		{
			Key:   []byte("a"),
			Value: []byte("a1"),
		},
		{
			Key:   []byte("a"),
			Value: []byte("a2"),
		},
	}
	c.handleMessages(context.TODO(), msgs)

	assert.NotNil(t, results)
	assert.Equal(t, []string{"a1", "a2"}, results)
}

func Test_Paral_MultiKey(t *testing.T) {
	log.Init(nil)
	c := &consumer{
		options: &ConsumerOptions{
			ParalSize:   2,
			OrderedMode: true,
		},
		random: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	c.pool, _ = ants.NewPool(c.options.ParalSize)

	var results []string
	var mtx sync.Mutex
	c.handler = func(ctx context.Context, msg *Message) error {
		mtx.Lock()
		defer mtx.Unlock()

		results = append(results, string(msg.Value))
		return nil
	}

	msgs := []kafka.Message{
		{
			Key:   []byte("a"),
			Value: []byte("a1"),
		},
		{
			Key:   []byte("a"),
			Value: []byte("a2"),
		},
		{
			Key:   []byte("c"),
			Value: []byte("c1"),
		},
		{
			Key:   []byte("b"),
			Value: []byte("b1"),
		},
		{
			Key:   []byte("b"),
			Value: []byte("b2"),
		},
	}
	c.handleMessages(context.TODO(), msgs)

	assert.NotNil(t, results)
	assert.Equal(t, len(msgs), len(results))
	assert.Less(t, slices.Index(results, "a1"), slices.Index(results, "a2"))
	assert.Less(t, slices.Index(results, "b1"), slices.Index(results, "b2"))
}
