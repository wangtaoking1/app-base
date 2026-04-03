// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka_confluent

import (
	"context"
	"math/rand"
	"sync"

	"github.com/wangtaoking1/app-base/utils"
)

// ConsumeQueue buffers and dispatches incoming messages to handlers.
type ConsumeQueue interface {
	Run(ctx context.Context)
	Add(ctx context.Context, msgID int64, msg *rawMessage)
}

// messageHandler is the internal per-message processing function.
type messageHandler func(ctx context.Context, msgID int64, msg *rawMessage)

type queueItem struct {
	msgID int64
	msg   *rawMessage
}

// unorderedQueue dispatches messages to workers in arbitrary order.
type unorderedQueue struct {
	workerCount int
	inQ         chan *queueItem
	handler     messageHandler
}

var _ ConsumeQueue = (*unorderedQueue)(nil)

func newUnorderedQueue(cacheSize, workerCount int, handler messageHandler) *unorderedQueue {
	return &unorderedQueue{
		workerCount: workerCount,
		inQ:         make(chan *queueItem, cacheSize*workerCount),
		handler:     handler,
	}
}

func (q *unorderedQueue) Add(ctx context.Context, msgID int64, msg *rawMessage) {
	item := &queueItem{msgID: msgID, msg: msg}
	select {
	case <-ctx.Done():
		return
	case q.inQ <- item:
	}
}

func (q *unorderedQueue) Run(ctx context.Context) {
	for range q.workerCount {
		go func(ch chan *queueItem) {
			for {
				select {
				case <-ctx.Done():
					return
				case it, ok := <-ch:
					if !ok {
						return
					}
					q.handler(ctx, it.msgID, it.msg)
				}
			}
		}(q.inQ)
	}
	<-ctx.Done()
}

// orderedQueue ensures messages with the same key are processed in order
// by routing them to a dedicated worker via key hashing.
type orderedQueue struct {
	workerCount int
	inQ         chan *queueItem
	workerQs    []chan *queueItem
	handler     messageHandler
}

var _ ConsumeQueue = (*orderedQueue)(nil)

func newOrderedQueue(cacheSize, workerCount int, handler messageHandler) *orderedQueue {
	workerQs := make([]chan *queueItem, workerCount)
	for i := range workerCount {
		workerQs[i] = make(chan *queueItem, cacheSize)
	}
	return &orderedQueue{
		workerCount: workerCount,
		inQ:         make(chan *queueItem, cacheSize),
		workerQs:    workerQs,
		handler:     handler,
	}
}

func (q *orderedQueue) Add(ctx context.Context, msgID int64, msg *rawMessage) {
	item := &queueItem{msgID: msgID, msg: msg}
	select {
	case <-ctx.Done():
		return
	case q.inQ <- item:
	}
}

func (q *orderedQueue) Run(ctx context.Context) {
	for i := range q.workerCount {
		go func(ch chan *queueItem) {
			for {
				select {
				case <-ctx.Done():
					return
				case it, ok := <-ch:
					if !ok {
						return
					}
					q.handler(ctx, it.msgID, it.msg)
				}
			}
		}(q.workerQs[i])
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		q.dispatch(ctx)
	}()

	<-ctx.Done()
	close(q.inQ)
	wg.Wait() // ensure dispatch has exited before closing worker queues
	for _, wq := range q.workerQs {
		close(wq)
	}
}

func (q *orderedQueue) dispatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case it, ok := <-q.inQ:
			if !ok {
				return
			}
			idx := q.hashByKey(string(it.msg.key), q.workerCount)
			select {
			case <-ctx.Done():
				return
			case q.workerQs[idx] <- it:
			}
		}
	}
}

func (q *orderedQueue) hashByKey(key string, cnt int) int {
	if key == "" {
		return rand.Intn(cnt)
	}
	return int(utils.StringHash(key) % uint32(cnt))
}
