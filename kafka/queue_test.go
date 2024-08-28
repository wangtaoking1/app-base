// Copyright 2024 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
)

func TestOffsetManager(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var commitedCnt atomic.Int32
	om := newOffsetManager(func(ctx context.Context, msgs []kafka.Message) error {
		commitedCnt.Add(int32(len(msgs)))
		return nil
	})
	om.commitInterval = 5 * time.Millisecond
	go om.Run(ctx)
	time.Sleep(1 * time.Millisecond)

	totalCount := 10
	var msgIDs []int64
	for i := 0; i < totalCount; i++ {
		seqID := om.addMessage(&kafka.Message{})
		msgIDs = append(msgIDs, seqID)
	}
	wg := sync.WaitGroup{}
	wg.Add(len(msgIDs))
	for i, seqID := range msgIDs {
		go func(c, id int64) {
			time.Sleep(time.Duration(c) * time.Millisecond)
			om.finish(id)
			wg.Done()
		}(int64(i), seqID)
	}
	wg.Wait()
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, 0, om.finished.Size())
	assert.Equal(t, 0, len(om.msgs))
}

func TestOffsetManager_ErrorCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	om := newOffsetManager(func(ctx context.Context, msgs []kafka.Message) error {
		return errors.New("commit error")
	})
	om.commitInterval = 5 * time.Millisecond
	go om.Run(ctx)
	time.Sleep(1 * time.Millisecond)

	totalCount := 10
	var msgIDs []int64
	for i := 0; i < totalCount; i++ {
		seqID := om.addMessage(&kafka.Message{})
		msgIDs = append(msgIDs, seqID)
	}
	wg := sync.WaitGroup{}
	wg.Add(len(msgIDs))
	for i, seqID := range msgIDs {
		go func(c, id int64) {
			time.Sleep(time.Duration(c) * time.Millisecond)
			om.finish(id)
			wg.Done()
		}(int64(i), seqID)
	}
	wg.Wait()
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, int64(-1), om.commitedID)
	assert.Equal(t, totalCount, len(om.msgs))
}

func TestOrderedQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var commitedCnt atomic.Int32
	commitFunc := func(ctx context.Context, msgs []kafka.Message) error {
		commitedCnt.Add(int32(len(msgs)))
		return nil
	}

	var results []string
	var mtx sync.Mutex
	handler := func(ctx context.Context, msg *kafka.Message) {
		mtx.Lock()
		defer mtx.Unlock()

		results = append(results, string(msg.Value))
	}

	q := newOrderedQueue(5, 5, handler, commitFunc)
	q.offsetMgr.commitInterval = 5 * time.Millisecond
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
		q.Add(msg)
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
	commitFunc := func(ctx context.Context, msgs []kafka.Message) error {
		commitedCnt.Add(int32(len(msgs)))
		return nil
	}

	var results []string
	var mtx sync.Mutex
	handler := func(ctx context.Context, msg *kafka.Message) {
		mtx.Lock()
		defer mtx.Unlock()

		results = append(results, string(msg.Value))
	}

	q := newUnorderedQueue(5, 5, handler, commitFunc)
	q.offsetMgr.commitInterval = 5 * time.Millisecond
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
		q.Add(msg)
	}

	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, len(msgs), len(results))
	assert.Equal(t, len(msgs), int(commitedCnt.Load()))
}
