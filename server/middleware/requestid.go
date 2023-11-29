// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package middleware

import (
	"github.com/gin-gonic/gin"
	uuid "github.com/satori/go.uuid"
)

const (
	// XRequestIDKey defines X-Request-ID key string.
	XRequestIDKey = "X-Request-ID"
)

func init() {
	Register("requestid", RequestID())
}

// RequestID is a middleware that injects a 'X-Request-ID' into the context and request/response header of each request.
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Check for incoming header, use it if exists
		rid := c.GetHeader(XRequestIDKey)

		if rid == "" {
			rid = uuid.NewV4().String()
			c.Request.Header.Set(XRequestIDKey, rid)
		}

		c.Set(XRequestIDKey, rid)
		// Set XRequestIDKey header
		c.Writer.Header().Set(XRequestIDKey, rid)

		c.Next()
	}
}

// GetRequestIDFromContext returns 'RequestID' from the given context if present.
func GetRequestIDFromContext(c *gin.Context) string {
	if v, ok := c.Get(XRequestIDKey); ok {
		if requestID, ok := v.(string); ok {
			return requestID
		}
	}

	return ""
}

// GetRequestIDFromHeaders returns 'RequestID' from the headers if present.
func GetRequestIDFromHeaders(c *gin.Context) string {
	return c.Request.Header.Get(XRequestIDKey)
}
