// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package heap

import (
	goheap "container/heap"
	"testing"
)

var (
	maxItemSize = 1000
)

func BenchmarkHeap_Push(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hp := New[int]()
		for j := maxItemSize; j >= 0; j-- {
			hp.Push(i)
		}
	}
}

func BenchmarkHeap_PushPop(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hp := New[int]()
		for j := maxItemSize; j >= 0; j-- {
			hp.Push(i)
		}
		for !hp.Empty() {
			hp.Pop()
		}
	}
}

type goHeap []int

func (h *goHeap) Len() int           { return len(*h) }
func (h *goHeap) Less(i, j int) bool { return (*h)[i] > (*h)[j] }
func (h *goHeap) Swap(i, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }
func (h *goHeap) Push(x interface{}) { *h = append(*h, x.(int)) }
func (h *goHeap) Pop() interface{} {
	n := len(*h)
	x := (*h)[n-1]
	*h = (*h)[:n-1]
	return x
}

func BenchmarkGoHeap_Push(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hp := &goHeap{}
		for j := maxItemSize; j >= 0; j-- {
			goheap.Push(hp, i)
		}
	}
}

func BenchmarkGoHeap_PushPop(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hp := &goHeap{}
		for j := maxItemSize; j >= 0; j-- {
			goheap.Push(hp, i)
		}
		for len(*hp) != 0 {
			goheap.Pop(hp)
		}
	}
}
