// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestOffsetManager(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var commitedCnt atomic.Int32
	om := newOffsetManagerWithFunc(nil, 5*time.Millisecond, func(ctx context.Context, msgs []kafka.Message) error {
		commitedCnt.Add(int32(len(msgs)))
		return nil
	})
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

	om := newOffsetManagerWithFunc(nil, 5*time.Millisecond, func(ctx context.Context, msgs []kafka.Message) error {
		return errors.New("commit error")
	})
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
