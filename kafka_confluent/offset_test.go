// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka_confluent

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOffsetManager_AllFinishInOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var committedCnt atomic.Int32
	om := newOffsetManager(5*time.Millisecond, func(_ context.Context, msgs []*rawMessage) error {
		committedCnt.Add(int32(len(msgs)))
		return nil
	})
	go om.Run(ctx)
	time.Sleep(time.Millisecond)

	const total = 10
	var ids []int64
	for range total {
		id := om.addMessage(&rawMessage{})
		ids = append(ids, id)
	}

	wg := sync.WaitGroup{}
	wg.Add(total)
	for i, id := range ids {
		go func(delay int, seqID int64) {
			time.Sleep(time.Duration(delay) * time.Millisecond)
			om.finish(seqID)
			wg.Done()
		}(i, id)
	}
	wg.Wait()
	time.Sleep(20 * time.Millisecond)

	assert.Equal(t, 0, om.finished.Size(), "finished set must be empty after all commits")
	assert.Equal(t, 0, len(om.msgs), "message map must be empty after all commits")
}

func TestOffsetManager_OutOfOrderFinish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var committedCnt atomic.Int32
	om := newOffsetManager(5*time.Millisecond, func(_ context.Context, msgs []*rawMessage) error {
		committedCnt.Add(int32(len(msgs)))
		return nil
	})
	go om.Run(ctx)
	time.Sleep(time.Millisecond)

	ids := make([]int64, 5)
	for i := range ids {
		ids[i] = om.addMessage(&rawMessage{})
	}

	// Finish in reverse order – offset must not advance past the first gap.
	om.finish(ids[4])
	om.finish(ids[3])
	om.finish(ids[2])
	time.Sleep(20 * time.Millisecond)

	// ids[0] and ids[1] are not finished – committed count should still be 0.
	assert.Equal(t, int64(-1), om.commitedID,
		"no contiguous prefix is complete yet; commitedID must remain -1")

	// Now finish the blocking messages.
	om.finish(ids[0])
	om.finish(ids[1])
	time.Sleep(20 * time.Millisecond)

	assert.Equal(t, int64(4), om.commitedID, "all messages should now be committed")
	assert.Equal(t, 0, len(om.msgs))
}

func TestOffsetManager_ErrorCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	commitErr := errors.New("broker unavailable")
	om := newOffsetManager(5*time.Millisecond, func(_ context.Context, _ []*rawMessage) error {
		return commitErr
	})
	go om.Run(ctx)
	time.Sleep(time.Millisecond)

	const total = 5
	var ids []int64
	for range total {
		id := om.addMessage(&rawMessage{})
		ids = append(ids, id)
	}
	for _, id := range ids {
		om.finish(id)
	}
	time.Sleep(20 * time.Millisecond)

	// Commit failed – status must not advance.
	assert.Equal(t, int64(-1), om.commitedID, "commitedID must remain -1 on commit failure")
	assert.Equal(t, total, len(om.msgs), "messages must be retained on commit failure")
}

func TestCommitOffsetsByPartition(t *testing.T) {
	msgs := []*rawMessage{
		{topic: "t1", partition: 0, offset: 10},
		{topic: "t1", partition: 0, offset: 12},
		{topic: "t1", partition: 1, offset: 5},
		{topic: "t2", partition: 0, offset: 3},
	}
	offsets := commitOffsetsByPartition(msgs)

	// Build a map for easy assertion.
	type key struct {
		topic     string
		partition int32
	}
	result := make(map[key]int64)
	for _, po := range offsets {
		result[key{po.Topic, po.Partition}] = po.Offset
	}

	assert.Equal(t, int64(13), result[key{"t1", 0}], "max offset for t1/p0 is 12, commit offset = 13")
	assert.Equal(t, int64(6), result[key{"t1", 1}], "t1/p1 only has offset 5, commit offset = 6")
	assert.Equal(t, int64(4), result[key{"t2", 0}], "t2/p0 only has offset 3, commit offset = 4")
}

func TestOffsetManager_NilCommitFuncIsNoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// nil commitFunc should not panic.
	om := newOffsetManager(5*time.Millisecond, nil)
	go om.Run(ctx)
	time.Sleep(time.Millisecond)

	id := om.addMessage(&rawMessage{})
	om.finish(id)
	time.Sleep(20 * time.Millisecond)
	// No panic means the test passed.
}
