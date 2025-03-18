// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/wangtaoking1/app-base/log"
	"github.com/wangtaoking1/app-base/utils"
	"github.com/wangtaoking1/go-common/container/set"
)

type offsetCommiter func(ctx context.Context, messages []kafka.Message) error

type offsetManager struct {
	mtx        sync.Mutex
	globalID   int64
	nextID     int64
	commitedID int64

	commitInterval time.Duration
	reader         *kafka.Reader
	commitFunc     offsetCommiter

	msgs     map[int64]*kafka.Message
	finished set.Set[int64]
}

func newOffsetManager(reader *kafka.Reader, commitInterval time.Duration) *offsetManager {
	return newOffsetManagerWithFunc(reader, commitInterval, nil)
}

func newOffsetManagerWithFunc(
	reader *kafka.Reader,
	commitInterval time.Duration,
	commitFunc offsetCommiter,
) *offsetManager {
	om := &offsetManager{
		commitedID:     -1,
		commitFunc:     commitFunc,
		reader:         reader,
		msgs:           make(map[int64]*kafka.Message),
		finished:       set.New[int64](),
		commitInterval: commitInterval,
	}
	if om.commitFunc == nil {
		om.commitFunc = om.commitOffset
	}
	return om
}

func (m *offsetManager) Run(ctx context.Context) {
	t := time.NewTimer(m.commitInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
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

	// Do commit messages
	if err := m.commitFunc(ctx, msgs); err != nil {
		return
	}

	// Refresh commited status
	m.refreshStatus(commitedID)
}

func (m *offsetManager) commitOffset(ctx context.Context, messages []kafka.Message) error {
	err := utils.LimitRetry(defaultRetryLimit, defaultRetryInterval, func() error {
		return m.reader.CommitMessages(ctx, messages...)
	})
	if err != nil {
		log.Errorw("Error commit offset to kafka", "error", err)
		return err
	}
	return nil
}

func (m *offsetManager) getCommitMsgs() (int64, []kafka.Message) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for m.nextID < m.globalID {
		if !m.finished.Contains(m.nextID) {
			break
		}
		m.finished.Remove(m.nextID)
		m.nextID++
	}
	var msgs []kafka.Message
	for seqID := m.commitedID + 1; seqID < m.nextID; seqID++ {
		msgs = append(msgs, *m.msgs[seqID])
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

func (m *offsetManager) addMessage(msg *kafka.Message) int64 {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	seqID := m.globalID
	m.globalID += 1
	m.msgs[seqID] = msg
	return seqID
}

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
