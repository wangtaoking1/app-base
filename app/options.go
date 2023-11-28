// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package app

import "github.com/wangtaoking1/app-base/flag"

// CmdOptions abstracts configuration options for reading parameters from the
// command line.
type CmdOptions interface {
	// Flags returns all FlagSets of command by sectioned.
	Flags() (fss flag.NamedFlagSets)
	// Validate validates the options fields.
	Validate() []error
}

// CompleteableOptions abstracts options which can be completed.
type CompleteableOptions interface {
	// Complete completes the options fields.
	Complete() error
}

// PrintableOptions abstracts options which can be printed.
type PrintableOptions interface {
	String() string
}
