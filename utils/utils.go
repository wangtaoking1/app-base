// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package utils

import (
	"hash/adler32"

	"golang.org/x/exp/constraints"
)

// StringHash hash a string key to unint32.
func StringHash(key string) uint32 {
	return adler32.Checksum([]byte(key))
}

// Min returns the smaller one.
func Min[E constraints.Ordered](a, b E) E {
	if a <= b {
		return a
	}
	return b
}

// Max returns the larger one.
func Max[E constraints.Ordered](a, b E) E {
	if a >= b {
		return a
	}
	return b
}
