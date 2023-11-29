// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/wangtaoking1/app-base/log"
	"github.com/wangtaoking1/app-base/server"
)

func main() {
	options := server.NewOptions()
	options.Profiling = true
	apiserver := server.New(options)
	if err := apiserver.Setup(initRouter); err != nil {
		log.Fatal(err.Error())
	}
	if err := apiserver.Run(); err != nil {
		log.Fatal(err.Error())
	}
}

func initRouter(g *gin.Engine) error {
	g.GET("/books/:name", getBooksHandler)

	return nil
}

type Book struct {
	Name string `json:"name"`
}

func getBooksHandler(c *gin.Context) {
	name := c.Param("name")
	book := Book{name}
	c.JSON(http.StatusOK, book)
}
