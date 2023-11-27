package heap

import (
	"cmp"
	goheap "container/heap"
)

// Heap returns a Heap interface.
type Heap[T any] interface {
	Push(T)
	Pop() T
	Size() int
	Empty() bool
}

type heap[T any] struct {
	s *internalHeap[T]
}

// New returns a new Heap container.
func New[T cmp.Ordered]() Heap[T] {
	less := func(e1, e2 T) bool {
		return cmp.Less(e1, e2)
	}
	return &heap[T]{
		s: newHeapSlice[T](less),
	}
}

// NewWithLess returns a new Heap container with custom less function.
func NewWithLess[T any](less func(e1, e2 T) bool) Heap[T] {
	if less == nil {
		return nil
	}
	return &heap[T]{
		s: newHeapSlice[T](less),
	}
}

func (h *heap[T]) Push(t T) {
	goheap.Push(h.s, t)
}

func (h *heap[T]) Pop() T {
	return goheap.Pop(h.s).(T)
}

func (h *heap[T]) Size() int {
	return h.s.Len()
}

func (h *heap[T]) Empty() bool {
	return h.Size() == 0
}

type internalHeap[T any] struct {
	s      []T
	lessFn func(e1, e2 T) bool
}

func newHeapSlice[T any](lessFn func(e1, e2 T) bool) *internalHeap[T] {
	return &internalHeap[T]{
		s:      make([]T, 0),
		lessFn: lessFn,
	}
}

func (hs *internalHeap[T]) Len() int {
	return len(hs.s)
}
func (hs *internalHeap[T]) Less(i, j int) bool {
	return hs.lessFn(hs.s[i], hs.s[j])
}
func (hs *internalHeap[T]) Swap(i, j int) {
	hs.s[i], hs.s[j] = hs.s[j], hs.s[i]
}
func (hs *internalHeap[T]) Push(item any) {
	hs.s = append(hs.s, item.(T))
}
func (hs *internalHeap[T]) Pop() any {
	n := len(hs.s)
	item := hs.s[n-1]
	hs.s = hs.s[:n-1]
	return item
}
