// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package mysql

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"
	gormlogger "gorm.io/gorm/logger"
)

// Options defines options for mysql database.
type Options struct {
	Host                  string        `json:"host,omitempty"                     mapstructure:"host"`
	Username              string        `json:"username,omitempty"                 mapstructure:"username"`
	Password              string        `json:"-"                                  mapstructure:"password"`
	Database              string        `json:"database"                           mapstructure:"database"`
	MaxIdleConnections    int           `json:"max-idle-connections,omitempty"     mapstructure:"max-idle-connections"`
	MaxOpenConnections    int           `json:"max-open-connections,omitempty"     mapstructure:"max-open-connections"`
	MaxConnectionLifeTime time.Duration `json:"max-connection-life-time,omitempty" mapstructure:"max-connection-life-time"`
	SlowThreshold         time.Duration `json:"slow-threshold,omitempty"           mapstructure:"slow-threshold"`
	LogLevel              int           `json:"log-level"                          mapstructure:"log-level"`
}

// NewOptions create a new options instance.
func NewOptions() *Options {
	return &Options{
		Host:                  "127.0.0.1:3306",
		Username:              "",
		Password:              "",
		Database:              "",
		MaxIdleConnections:    100,
		MaxOpenConnections:    100,
		MaxConnectionLifeTime: 10 * time.Second,
		SlowThreshold:         200 * time.Millisecond,
		LogLevel:              int(gormlogger.Silent),
	}
}

// Validate verifies flags passed to Options.
func (o *Options) Validate() []error {
	var errs []error
	if o.MaxIdleConnections < 0 {
		errs = append(errs, fmt.Errorf("max-idle-connections must not be a negative number"))
	}
	if o.MaxOpenConnections < o.MaxIdleConnections {
		errs = append(errs, fmt.Errorf("max-open-connections must not be less than max-idle-connections"))
	}
	if o.LogLevel < 1 || o.LogLevel > 4 {
		errs = append(errs, fmt.Errorf("log-level must be an integer in [1,4]"))
	}

	return errs
}

// AddFlags adds flags related to mysql storage for a specific APIServer to the specified FlagSet.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.Host, "mysql.host", o.Host, "MySQL service host address.")
	fs.StringVar(&o.Username, "mysql.username", o.Username, "Username for access to mysql service.")
	fs.StringVar(&o.Password, "mysql.password", o.Password, ""+
		"Password for access to mysql, should be used pair with password.")
	fs.StringVar(&o.Database, "mysql.database", o.Database, "Database name for the server to use.")

	fs.IntVar(&o.MaxIdleConnections, "mysql.max-idle-connections", o.MaxIdleConnections, ""+
		"Maximum idle connections allowed to connect to mysql.")
	fs.IntVar(&o.MaxOpenConnections, "mysql.max-open-connections", o.MaxOpenConnections, ""+
		"Maximum open connections allowed to connect to mysql.")
	fs.DurationVar(&o.MaxConnectionLifeTime, "mysql.max-connection-life-time", o.MaxConnectionLifeTime, ""+
		"Maximum connection life time allowed to connect to mysql.")
	fs.DurationVar(&o.SlowThreshold, "mysql.slow-threshold", o.SlowThreshold, ""+
		"Slow sql threshold when access to mysql.")

	fs.IntVar(&o.LogLevel, "mysql.log-level", o.LogLevel, "Specify gorm log level. "+
		"1: Silent, 2: Error, 3: Warn, 4: Info")
}
