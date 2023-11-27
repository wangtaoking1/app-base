package set

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSet_Add(t *testing.T) {
	s := New("1")
	assert.Equal(t, s.Size(), 1)
	assert.True(t, s.Contains("1"))

	s.Add("2")
	assert.Equal(t, s.Size(), 2)

	s.Add("2")
	assert.Equal(t, s.Size(), 2)

	s.AddAll("3", "4")
	assert.Equal(t, s.Size(), 4)
}

func TestSet_Remove(t *testing.T) {
	s := New(1, 2, 3)
	s.Remove(1)
	assert.Equal(t, s.Size(), 2)

	s.RemoveAll(2, 3)
	assert.Equal(t, s.Size(), 0)
	assert.True(t, s.Empty())
}

func TestSet_Clear(t *testing.T) {
	s := New(1, 2, 3)
	assert.False(t, s.Empty())

	s.Clear()
	assert.True(t, s.Empty())
}

func TestSet_Size(t *testing.T) {
	tests := []struct {
		name     string
		elements []int
		want     int
	}{
		{
			name:     "nil set",
			elements: nil,
			want:     0,
		},
		{
			name:     "no elements",
			elements: []int{},
			want:     0,
		},
		{
			name:     "n elements",
			elements: []int{1, 3},
			want:     2,
		},
		{
			name:     "duplicate elements",
			elements: []int{1, 1},
			want:     1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New(tt.elements...)
			assert.Equal(t, tt.want, s.Size())
		})
	}
}

func TestSet_Values(t *testing.T) {
	d := []int{1, 2, 3}
	s := New(d...)
	rd := s.Values()
	slices.Sort(rd)
	assert.Equal(t, d, rd)
}
