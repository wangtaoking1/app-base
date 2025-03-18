// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"math/rand"

	"github.com/segmentio/kafka-go"
	"github.com/wangtaoking1/app-base/utils"
)

type ConsumeQueue interface {
	Run(ctx context.Context)
	Add(ctx context.Context, msgID int64, msg *kafka.Message)
}

type messageHandler func(ctx context.Context, msgID int64, msg *kafka.Message)

type queueItem struct {
	msgID int64
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

func newUnorderedQueue(cacheSize, workerCount int, handler messageHandler) *unorderedQueue {
	return &unorderedQueue{
		workerCount: workerCount,
		cacheSize:   cacheSize,
		inQ:         make(chan *queueItem, cacheSize*workerCount),
		handler:     handler,
	}
}

func (q *unorderedQueue) Add(ctx context.Context, msgID int64, msg *kafka.Message) {
	it := &queueItem{msgID: msgID, msg: msg}

	select {
	case <-ctx.Done():
		return
	case q.inQ <- it:
	}
}

func (q *unorderedQueue) Run(ctx context.Context) {
	for i := 0; i < q.workerCount; i++ {
		go func(ch chan *queueItem) {
			for {
				select {
				case <-ctx.Done():
					return
				case it := <-ch:
					q.handler(ctx, it.msgID, it.msg)
				}
			}
		}(q.inQ)
	}

	<-ctx.Done()
	close(q.inQ)
}

type orderedQueue struct {
	workerCount int
	cacheSize   int
	inQ         chan *queueItem
	workerQs    []chan *queueItem
	handler     messageHandler
}

var _ ConsumeQueue = (*orderedQueue)(nil)

func newOrderedQueue(cacheSize, workerCount int, handler messageHandler) *orderedQueue {
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
	}
}

func (q *orderedQueue) Add(ctx context.Context, msgID int64, msg *kafka.Message) {
	it := &queueItem{msgID: msgID, msg: msg}

	select {
	case <-ctx.Done():
		return
	case q.inQ <- it:
	}
}

func (q *orderedQueue) Run(ctx context.Context) {
	for i := 0; i < q.workerCount; i++ {
		go func(ch chan *queueItem) {
			for {
				select {
				case <-ctx.Done():
					return
				case it := <-ch:
					q.handler(ctx, it.msgID, it.msg)
				}
			}
		}(q.workerQs[i])
	}

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
		return rand.Intn(cnt)
	}
	return int(utils.StringHash(key) % uint32(cnt))
}
