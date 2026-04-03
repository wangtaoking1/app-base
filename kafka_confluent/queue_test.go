// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka_confluent

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
)

func makeRaw(key, value string) *rawMessage {
	return &rawMessage{key: []byte(key), value: []byte(value)}
}

func TestOrderedQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var committedCnt atomic.Int32
	om := newOffsetManager(5*time.Millisecond, func(_ context.Context, msgs []*rawMessage) error {
		committedCnt.Add(int32(len(msgs)))
		return nil
	})
	go om.Run(ctx)

	var mu sync.Mutex
	var results []string
	handler := func(_ context.Context, msgID int64, msg *rawMessage) {
		mu.Lock()
		defer mu.Unlock()
		results = append(results, string(msg.value))
		om.finish(msgID)
	}

	q := newOrderedQueue(5, 5, handler)
	go q.Run(ctx)
	time.Sleep(time.Millisecond)

	msgs := []*rawMessage{
		makeRaw("a", "a1"),
		makeRaw("a", "a2"),
		makeRaw("c", "c1"),
		makeRaw("b", "b1"),
		makeRaw("b", "b2"),
	}
	for _, m := range msgs {
		id := om.addMessage(m)
		q.Add(ctx, id, m)
	}

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, len(msgs), len(results), "all messages must be processed")
	assert.Equal(t, len(msgs), int(committedCnt.Load()), "all messages must be committed")

	// Same-key messages must arrive in submission order.
	assert.Less(t, slices.Index(results, "a1"), slices.Index(results, "a2"),
		"a1 must be processed before a2")
	assert.Less(t, slices.Index(results, "b1"), slices.Index(results, "b2"),
		"b1 must be processed before b2")
}

func TestUnorderedQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var committedCnt atomic.Int32
	om := newOffsetManager(5*time.Millisecond, func(_ context.Context, msgs []*rawMessage) error {
		committedCnt.Add(int32(len(msgs)))
		return nil
	})
	go om.Run(ctx)

	var mu sync.Mutex
	var results []string
	handler := func(_ context.Context, msgID int64, msg *rawMessage) {
		mu.Lock()
		defer mu.Unlock()
		results = append(results, string(msg.value))
		om.finish(msgID)
	}

	q := newUnorderedQueue(5, 5, handler)
	go q.Run(ctx)
	time.Sleep(time.Millisecond)

	msgs := []*rawMessage{
		makeRaw("a", "a1"),
		makeRaw("a", "a2"),
		makeRaw("c", "c1"),
		makeRaw("b", "b1"),
		makeRaw("b", "b2"),
	}
	for _, m := range msgs {
		id := om.addMessage(m)
		q.Add(ctx, id, m)
	}

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, len(msgs), len(results), "all messages must be processed")
	assert.Equal(t, len(msgs), int(committedCnt.Load()), "all messages must be committed")
}

func TestOrderedQueue_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var processed atomic.Int32
	handler := func(_ context.Context, _ int64, _ *rawMessage) {
		processed.Add(1)
	}
	om := newOffsetManager(time.Second, nil)
	q := newOrderedQueue(5, 2, handler)
	go q.Run(ctx)
	time.Sleep(time.Millisecond)

	for i := range 5 {
		_ = i
		m := makeRaw("k", "v")
		id := om.addMessage(m)
		q.Add(ctx, id, m)
	}
	time.Sleep(20 * time.Millisecond)
	cancel()
	// Should not block
}

func TestUnorderedQueue_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	handler := func(_ context.Context, _ int64, _ *rawMessage) {}
	om := newOffsetManager(time.Second, nil)
	q := newUnorderedQueue(5, 2, handler)
	go q.Run(ctx)
	time.Sleep(time.Millisecond)

	for range 5 {
		m := makeRaw("k", "v")
		id := om.addMessage(m)
		q.Add(ctx, id, m)
	}
	time.Sleep(20 * time.Millisecond)
	cancel()
}
