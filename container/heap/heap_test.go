package heap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHeap(t *testing.T) {
	hp := New[int]()
	hp.Push(3)
	hp.Push(2)
	hp.Push(5)
	assert.Equal(t, 2, hp.Pop())
	assert.Equal(t, 3, hp.Pop())
	assert.Equal(t, 1, hp.Size())
}

func TestHeap_Big(t *testing.T) {
	hp := NewWithLess(func(e1, e2 int) bool {
		return e1 > e2
	})
	hp.Push(3)
	hp.Push(2)
	hp.Push(5)
	assert.Equal(t, 5, hp.Pop())
	assert.Equal(t, 3, hp.Pop())
	assert.Equal(t, 1, hp.Size())
}

func TestHeap_Custom(t *testing.T) {
	type cType struct {
		a, b int
	}
	hp := NewWithLess(func(e1, e2 *cType) bool {
		return e1.a < e2.a
	})

	hp.Push(&cType{3, 3})
	hp.Push(&cType{1, 1})
	hp.Push(&cType{2, 2})
	assert.Equal(t, 1, hp.Pop().a)
}

func TestHeap_Empty(t *testing.T) {
	hp := New[string]()
	assert.True(t, hp.Empty())

	hp.Push("a")
	assert.False(t, hp.Empty())
}

func TestHeap_Nil_Less(t *testing.T) {
	hp := NewWithLess[string](nil)
	assert.Equal(t, nil, hp)
}
