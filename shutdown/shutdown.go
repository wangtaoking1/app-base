// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package shutdown

import (
	"sync"

	"github.com/wangtaoking1/app-base/errors"
)

// Callback is an interface you have to implement for callbacks.
type Callback interface {
	// OnShutdown will be called when shutdown is triggered. The parameter
	// is the name of the shutdown trigger that trigger shutdown.
	OnShutdown(string) error
}

// CallbackFunc is a helper type, so you can easily provide anonymous functions
// as shutdown Callbacks.
type CallbackFunc func(string) error

func (f CallbackFunc) OnShutdown(trigger string) error {
	return f(trigger)
}

// ErrorHandler is an interface you can pass to SetErrorHandler to
// handle asynchronous errors.
type ErrorHandler interface {
	OnError(error)
}

// ErrorFunc is a helper type, so you can easily provide anonymous functions
// as ErrorHandlers.
type ErrorFunc func(err error)

// OnError defines the action needed to run when error occurred.
func (f ErrorFunc) OnError(err error) {
	f(err)
}

// Executor is the interface of execute func after triggering shutdown.
type Executor interface {
	Execute(Trigger)
}

// ExecuteFunc defines the execute func.
type ExecuteFunc func(Trigger)

func (f ExecuteFunc) Execute(trigger Trigger) {
	f(trigger)
}

// Trigger is an interface implemnted by shutdown triggers.
type Trigger interface {
	// GetName returns the name of the trigger.
	GetName() string
	// Start starts the trigger to listen some shutdown requests.
	Start(Executor) error
	// After fun do something after shutdown, like exit.
	After()
}

// Shutdown is an interface implemented by shutdownController,
// that receives shutdown triggers when shutdown is requested.
type Shutdown interface {
	// Start starts the graceful shutdown controller.
	Start() error
	// AddCallback adds callback func to the shutdown controller.
	AddCallback(Callback)
	// SetErrorHandler set errorHandler for the shutdown controller.
	SetErrorHandler(ErrorHandler)
}

type shutdownController struct {
	triggers     []Trigger
	callbacks    []Callback
	errorHandler ErrorHandler
}

// New returns a new graceful shutdown instance with the specified triggers.
func New(triggers ...Trigger) Shutdown {
	return &shutdownController{
		triggers:  triggers,
		callbacks: make([]Callback, 0, 1),
	}
}

func (g *shutdownController) AddCallback(cb Callback) {
	g.callbacks = append(g.callbacks, cb)
}

func (g *shutdownController) SetErrorHandler(h ErrorHandler) {
	g.errorHandler = h
}

func (g *shutdownController) Start() error {
	for _, t := range g.triggers {
		if err := t.Start(g.executeFunc()); err != nil {
			return errors.WithMessagef(err, "start shutdown trigger %s error", t.GetName())
		}
	}

	return nil
}

func (g *shutdownController) executeFunc() Executor {
	return ExecuteFunc(func(trigger Trigger) {
		var wg sync.WaitGroup
		for _, cb := range g.callbacks {
			wg.Add(1)
			go func(callback Callback) {
				defer wg.Done()

				g.handleError(callback.OnShutdown(trigger.GetName()))
			}(cb)
		}

		wg.Wait()

		trigger.After()
	})
}

func (g *shutdownController) handleError(err error) {
	if err == nil || g.errorHandler == nil {
		return
	}
	g.errorHandler.OnError(err)
}
