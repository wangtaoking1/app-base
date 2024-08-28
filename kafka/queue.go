// Copyright 2024 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/wangtaoking1/app-base/utils"
	"github.com/wangtaoking1/go-common/container/set"
)

const (
	defaultCommitInterval = 2 * time.Second
)

type ConsumeQueue interface {
	Run(ctx context.Context)
	Add(msg *kafka.Message)
}

type (
	messageHandler func(ctx context.Context, msg *kafka.Message)
	offsetCommiter func(ctx context.Context, messages []kafka.Message) error
)

type queueItem struct {
	seqID int64
	msg   *kafka.Message
}

type unorderedQueue struct {
	workerCount int
	cacheSize   int
	inQ         chan *queueItem
	handler     messageHandler
	offsetMgr   *offsetManager
}

var _ ConsumeQueue = (*unorderedQueue)(nil)

func newUnorderedQueue(cacheSize, workerCount int, handler messageHandler, commiter offsetCommiter) *unorderedQueue {
	return &unorderedQueue{
		workerCount: workerCount,
		cacheSize:   cacheSize,
		inQ:         make(chan *queueItem, cacheSize*workerCount),
		handler:     handler,
		offsetMgr:   newOffsetManager(commiter),
	}
}

func (q *unorderedQueue) Add(msg *kafka.Message) {
	seqID := q.offsetMgr.addMessage(msg)
	it := &queueItem{seqID: seqID, msg: msg}
	q.inQ <- it
}

func (q *unorderedQueue) Run(ctx context.Context) {
	for i := 0; i < q.workerCount; i++ {
		go func(ch chan *queueItem) {
			for {
				select {
				case <-ctx.Done():
					return
				case it := <-ch:
					q.handler(ctx, it.msg)
					q.offsetMgr.finish(it.seqID)
				}
			}
		}(q.inQ)
	}
	go q.offsetMgr.Run(ctx)

	<-ctx.Done()
	close(q.inQ)
}

type orderedQueue struct {
	workerCount int
	cacheSize   int
	inQ         chan *queueItem
	workerQs    []chan *queueItem
	handler     messageHandler
	offsetMgr   *offsetManager
}

var _ ConsumeQueue = (*orderedQueue)(nil)

func newOrderedQueue(cacheSize, workerCount int, handler messageHandler, commiter offsetCommiter) *orderedQueue {
	workerQs := make([]chan *queueItem, workerCount)
	for i := 0; i < workerCount; i++ {
		workerQs[i] = make(chan *queueItem, cacheSize)
	}
	return &orderedQueue{
		workerCount: workerCount,
		cacheSize:   cacheSize,
		inQ:         make(chan *queueItem, cacheSize),
		workerQs:    workerQs,
		handler:     handler,
		offsetMgr:   newOffsetManager(commiter),
	}
}

func (q *orderedQueue) Add(msg *kafka.Message) {
	seqID := q.offsetMgr.addMessage(msg)
	it := &queueItem{seqID: seqID, msg: msg}
	q.inQ <- it
}

func (q *orderedQueue) Run(ctx context.Context) {
	for i := 0; i < q.workerCount; i++ {
		go func(ch chan *queueItem) {
			for {
				select {
				case <-ctx.Done():
					return
				case it := <-ch:
					q.handler(ctx, it.msg)
					q.offsetMgr.finish(it.seqID)
				}
			}
		}(q.workerQs[i])
	}

	go q.offsetMgr.Run(ctx)
	go q.dispatch(ctx)

	<-ctx.Done()
	close(q.inQ)
	for _, q := range q.workerQs {
		close(q)
	}
}

func (q *orderedQueue) dispatch(ctx context.Context) {
	// dispatch messages from in queue to worker queue
	for {
		select {
		case <-ctx.Done():
			return
		case it := <-q.inQ:
			idx := q.hashByKey(string(it.msg.Key), q.workerCount)
			q.workerQs[idx] <- it
		}
	}
}

func (q *orderedQueue) hashByKey(key string, cnt int) int {
	if key == "" {
		return rand.Intn(cnt) //nolint:gosec
	}
	return int(utils.StringHash(key) % uint32(cnt))
}

type offsetManager struct {
	mtx        sync.Mutex
	globalID   int64
	nextID     int64
	commitedID int64

	commitInterval time.Duration
	commitFunc     offsetCommiter

	msgs     map[int64]*kafka.Message
	finished set.Set[int64]
}

func newOffsetManager(commitFunc offsetCommiter) *offsetManager {
	return &offsetManager{
		commitedID:     -1,
		commitFunc:     commitFunc,
		msgs:           make(map[int64]*kafka.Message),
		finished:       set.New[int64](),
		commitInterval: defaultCommitInterval,
	}
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

	// Refresh committed status
	m.refreshStatus(commitedID)
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

func (m *offsetManager) finish(seqID int64) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.msgs[seqID] == nil {
		return
	}
	m.finished.Add(seqID)
}
