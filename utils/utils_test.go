// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/constraints"
)

func TestMin(t *testing.T) {
	type args[E constraints.Ordered] struct {
		a E
		b E
	}
	type testCase[E constraints.Ordered] struct {
		name string
		args args[E]
		want E
	}
	tests := []testCase[int]{
		{
			name: "small",
			args: args[int]{a: 1, b: 2},
			want: 1,
		},
		{
			name: "reverse",
			args: args[int]{a: 2, b: 1},
			want: 1,
		},
		{
			name: "equal",
			args: args[int]{a: 1, b: 1},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, Min(tt.args.a, tt.args.b), "Min(%v, %v)", tt.args.a, tt.args.b)
		})
	}
}

func TestMax(t *testing.T) {
	type args[E constraints.Ordered] struct {
		a E
		b E
	}
	type testCase[E constraints.Ordered] struct {
		name string
		args args[E]
		want E
	}
	tests := []testCase[int]{
		{
			name: "small",
			args: args[int]{a: 1, b: 2},
			want: 2,
		},
		{
			name: "reverse",
			args: args[int]{a: 2, b: 1},
			want: 2,
		},
		{
			name: "equal",
			args: args[int]{a: 1, b: 1},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, Max(tt.args.a, tt.args.b), "Max(%v, %v)", tt.args.a, tt.args.b)
		})
	}
}
