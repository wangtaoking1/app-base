// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
)

func TestOrderedQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var commitedCnt atomic.Int32
	offsetMgr := newOffsetManagerWithFunc(
		nil,
		5*time.Millisecond,
		func(ctx context.Context, msgs []kafka.Message) error {
			commitedCnt.Add(int32(len(msgs)))
			return nil
		},
	)
	go offsetMgr.Run(ctx)

	var results []string
	var mtx sync.Mutex
	handler := func(ctx context.Context, msgID int64, msg *kafka.Message) {
		mtx.Lock()
		defer mtx.Unlock()

		results = append(results, string(msg.Value))
		offsetMgr.finish(msgID)
	}

	q := newOrderedQueue(5, 5, handler)
	go q.Run(ctx)
	time.Sleep(1 * time.Millisecond)

	msgs := []*kafka.Message{
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
	for _, msg := range msgs {
		msgID := offsetMgr.addMessage(msg)
		q.Add(ctx, msgID, msg)
	}

	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, len(msgs), len(results))
	assert.Equal(t, len(msgs), int(commitedCnt.Load()))
	assert.Less(t, slices.Index(results, "a1"), slices.Index(results, "a2"))
	assert.Less(t, slices.Index(results, "b1"), slices.Index(results, "b2"))
}

func TestUnorderedQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var commitedCnt atomic.Int32
	offsetMgr := newOffsetManagerWithFunc(
		nil,
		5*time.Millisecond,
		func(ctx context.Context, msgs []kafka.Message) error {
			commitedCnt.Add(int32(len(msgs)))
			return nil
		},
	)
	go offsetMgr.Run(ctx)

	var results []string
	var mtx sync.Mutex
	handler := func(ctx context.Context, msgID int64, msg *kafka.Message) {
		mtx.Lock()
		defer mtx.Unlock()

		results = append(results, string(msg.Value))
		offsetMgr.finish(msgID)
	}

	q := newUnorderedQueue(5, 5, handler)
	go q.Run(ctx)
	time.Sleep(1 * time.Millisecond)

	msgs := []*kafka.Message{
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
	for _, msg := range msgs {
		msgID := offsetMgr.addMessage(msg)
		q.Add(ctx, msgID, msg)
	}

	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, len(msgs), len(results))
	assert.Equal(t, len(msgs), int(commitedCnt.Load()))
}
