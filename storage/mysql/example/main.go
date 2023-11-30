// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package main

import (
	"github.com/wangtaoking1/app-base/log"
	"github.com/wangtaoking1/app-base/storage/mysql"
)

func main() {
	opts := mysql.NewOptions()
	opts.Username = ""
	opts.Password = ""
	opts.Database = ""
	db, err := mysql.New(opts)
	if err != nil {
		log.Fatal(err.Error())
	}

	var students []Student
	tx := db.Find(&students)
	if tx.Error != nil {
		log.Fatal(tx.Error.Error())
	}
	log.Info("Search finished", "students", students)
}

type Student struct {
	ID   int64  `gorm:"column:id"`
	Name string `gorm:"column:name"`
	Age  int    `gorm:"column:age"`
}

func (s *Student) TableName() string {
	return "students"
}
