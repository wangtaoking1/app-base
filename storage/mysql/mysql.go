// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package mysql

import (
	"fmt"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// New returns a new gorm db instance with specified options.
func New(opts *Options) (*gorm.DB, error) {
	dsn := fmt.Sprintf(`%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=%t&loc=%s`,
		opts.Username,
		opts.Password,
		opts.Host,
		opts.Database,
		true,
		"Local")

	db, err := gorm.Open(mysql.Open(dsn))
	if err != nil {
		return nil, err
	}

	if err := setupDBConfigs(db, opts); err != nil {
		return nil, err
	}

	return db, nil
}

func setupDBConfigs(db *gorm.DB, opts *Options) error {
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}

	sqlDB.SetMaxOpenConns(opts.MaxOpenConnections)
	sqlDB.SetConnMaxLifetime(opts.MaxConnectionLifeTime)
	sqlDB.SetMaxIdleConns(opts.MaxIdleConnections)

	return nil
}
