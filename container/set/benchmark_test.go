// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package set

import "testing"

var (
	maxSize = 10000
)

func BenchmarkSet_Add(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := New[int]()
		for x := 1; x <= maxSize; x++ {
			s.Add(x)
		}
	}
}

func BenchmarkSet_AddRemove(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := New[int]()
		for x := 1; x <= maxSize; x++ {
			s.Add(x)
		}
		for x := 1; x <= maxSize; x++ {
			s.Remove(x)
		}
	}
}

func BenchmarkMapSet_Add(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := make(map[int]struct{})
		for x := 1; x <= maxSize; x++ {
			s[x] = empty
		}
	}
}

func BenchmarkMapSet_AddRemove(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := make(map[int]struct{})
		for x := 1; x <= maxSize; x++ {
			s[x] = empty
		}
		for x := 1; x <= maxSize; x++ {
			delete(s, x)
		}
	}
}
