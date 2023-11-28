// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package set

import "fmt"

func ExampleSet() {
	s := New(1, 2)
	s.Add(3)
	s.Remove(2)
	fmt.Println("Contains 1:", s.Contains(1))
	fmt.Println("Contains 2:", s.Contains(2))
	fmt.Println("Empty:", s.Empty())

	// Output:
	// Contains 1: true
	// Contains 2: false
	// Empty: false
}
