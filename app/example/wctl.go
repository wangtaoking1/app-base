// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package main

import (
	"time"

	"github.com/wangtaoking1/app-base/app"
	"github.com/wangtaoking1/app-base/errors"
	"github.com/wangtaoking1/app-base/flag"
	"github.com/wangtaoking1/app-base/log"
)

type WOptions struct {
	Interval time.Duration `json:"interval" mapstructure:"interval"`

	Log *log.Options `json:"log" mapstructure:"log"`
}

func (o *WOptions) Flags() (fss flag.NamedFlagSets) {
	fs := fss.FlagSet("generic")
	fs.DurationVarP(&o.Interval, "interval", "i", 5*time.Second, "sync interval")

	o.Log.AddFlags(fss.FlagSet("log"))

	return fss
}

func (o *WOptions) Validate() []error {
	var errs []error
	if o.Interval > 30*time.Second {
		errs = append(errs, errors.New("interval must not bigger than 30s"))
	}

	errs = append(errs, o.Log.Validate()...)

	return errs
}

func newWOptions() *WOptions {
	return &WOptions{
		Interval: 5 * time.Second,

		Log: log.NewOptions(),
	}
}

func main() {
	options := newWOptions()
	application := app.NewApp("wctl",
		"w ctl",
		app.WithDescription("This is a w ctl just for test"),
		app.WithOptions(options),
		app.WithDefaultValidArgs(),
		app.WithCommands(commands...),
		app.WithRunFunc(run(options)),
	)

	application.Run()
}

func run(opts *WOptions) app.RunFunc {
	return func(name string) error {
		log.Init(opts.Log)

		log.Debug("This is a debug msg for test")
		log.Info("This is a hello world msg")
		log.Infof("The sync interval is %v", opts.Interval)

		return nil
	}
}
