// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka_confluent

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/wangtaoking1/app-base/log"
	"github.com/wangtaoking1/app-base/utils"
	"github.com/wangtaoking1/go-common/container/set"
)

// offsetCommitter commits a batch of finished messages to the broker.
// Implementations convert rawMessage partition/offset info to broker-specific calls.
type offsetCommitter func(ctx context.Context, msgs []*rawMessage) error

// offsetManager tracks in-flight messages and commits offsets in order.
// Messages are assigned a monotonic sequence ID; the manager commits only the
// highest contiguous finished sequence to avoid skipping un-acked messages.
type offsetManager struct {
	mtx        sync.Mutex
	globalID   int64
	nextID     int64
	commitedID int64

	commitInterval time.Duration
	commitFunc     offsetCommitter

	msgs     map[int64]*rawMessage
	finished set.Set[int64]
}

func newOffsetManager(commitInterval time.Duration, commitFunc offsetCommitter) *offsetManager {
	om := &offsetManager{
		commitedID:     -1,
		commitFunc:     commitFunc,
		msgs:           make(map[int64]*rawMessage),
		finished:       set.New[int64](),
		commitInterval: commitInterval,
	}
	if om.commitFunc == nil {
		om.commitFunc = func(_ context.Context, _ []*rawMessage) error { return nil }
	}
	return om
}

// Run periodically commits finished offsets until ctx is cancelled.
func (m *offsetManager) Run(ctx context.Context) {
	t := time.NewTimer(m.commitInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			m.doCommit(context.Background()) // final commit before exit
			return
		case <-t.C:
			m.doCommit(ctx)
			t.Reset(m.commitInterval)
		}
	}
}

func (m *offsetManager) doCommit(ctx context.Context) {
	commitedID, msgs := m.getCommitMsgs()
	if len(msgs) == 0 {
		return
	}
	if err := m.commitFunc(ctx, msgs); err != nil {
		return
	}
	m.refreshStatus(commitedID)
}

func (m *offsetManager) getCommitMsgs() (int64, []*rawMessage) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for m.nextID < m.globalID {
		if !m.finished.Contains(m.nextID) {
			break
		}
		m.finished.Remove(m.nextID)
		m.nextID++
	}

	var msgs []*rawMessage
	for seqID := m.commitedID + 1; seqID < m.nextID; seqID++ {
		msgs = append(msgs, m.msgs[seqID])
	}
	return m.nextID - 1, msgs
}

func (m *offsetManager) refreshStatus(commitedID int64) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for seqID := m.commitedID + 1; seqID <= commitedID; seqID++ {
		delete(m.msgs, seqID)
	}
	m.commitedID = commitedID
}

// addMessage registers a new message and returns its sequence ID.
func (m *offsetManager) addMessage(msg *rawMessage) int64 {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	seqID := m.globalID
	m.globalID++
	m.msgs[seqID] = msg
	return seqID
}

// finish marks a sequence of messages as processed, allowing offset advancement.
func (m *offsetManager) finish(seqIDs ...int64) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for _, seqID := range seqIDs {
		if m.msgs[seqID] == nil {
			continue
		}
		m.finished.Add(seqID)
	}
}

// commitOffsetsByPartition converts rawMessages to per-partition max offsets
// and commits them via the given confluent consumer.
// The committed offset is msg.offset+1 (Kafka convention: next offset to fetch).
func commitOffsetsByPartition(msgs []*rawMessage) []partitionOffset {
	type partKey struct {
		topic     string
		partition int32
	}
	maxOffsets := make(map[partKey]int64)
	for _, msg := range msgs {
		key := partKey{msg.topic, msg.partition}
		if cur, exists := maxOffsets[key]; !exists || msg.offset > cur {
			maxOffsets[key] = msg.offset
		}
	}

	result := make([]partitionOffset, 0, len(maxOffsets))
	for key, offset := range maxOffsets {
		result = append(result, partitionOffset{
			Topic:     key.topic,
			Partition: key.partition,
			Offset:    offset + 1, // next offset to fetch
		})
	}
	return result
}

// partitionOffset holds the commit target for a single partition.
type partitionOffset struct {
	Topic     string
	Partition int32
	Offset    int64
}

// defaultRetryLimit and defaultRetryInterval are used for commit retries.
const (
	defaultRetryLimit    = 3
	defaultRetryInterval = 100 * time.Millisecond
)

// retryCommit wraps a commit call with limit retry logic.
func retryCommit(ctx context.Context, f func() error) error {
	err := utils.LimitRetry(ctx, defaultRetryLimit, defaultRetryInterval, f)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		log.Errorw("Error committing kafka offset", "error", err)
		return err
	}
	return nil
}
