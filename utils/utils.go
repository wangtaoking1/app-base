// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package utils

import (
	"hash/adler32"
)

// StringHash hash a string key to unint32.
func StringHash(key string) uint32 {
	return adler32.Checksum([]byte(key))
}

// Ptr returns a pointer to its argument val.
func Ptr[T any](val T) *T {
	return &val
}
