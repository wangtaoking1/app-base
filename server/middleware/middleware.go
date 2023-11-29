// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package middleware

import (
	"sync"

	"github.com/gin-gonic/gin"
)

var (
	middlewares = map[string]gin.HandlerFunc{}
	mtx         sync.RWMutex
)

// Register register a middleware.
func Register(name string, middleware gin.HandlerFunc) {
	mtx.Lock()
	defer mtx.Unlock()

	middlewares[name] = middleware
}

// Get returns the specific name middleware.
func Get(name string) gin.HandlerFunc {
	mtx.RLock()
	defer mtx.RUnlock()

	return middlewares[name]
}
