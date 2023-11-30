// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"time"

	"github.com/wangtaoking1/app-base/log"
	"github.com/wangtaoking1/app-base/storage/etcd"
)

func main() {
	opts := etcd.NewOptions()
	opts.Username = ""
	opts.Password = ""
	opts.Namespace = "test"
	store := etcd.New(opts)
	defer store.Close()

	err := store.Put(context.TODO(), "aaa", "aaa")
	if err != nil {
		log.Fatal(err.Error())
	}

	v, err := store.Get(context.TODO(), "aaa")
	if err != nil {
		log.Fatal(err.Error())
	}
	log.Infof("value is %s", string(v))

	testWatch(store)

	time.Sleep(1000 * time.Second)
}

func testWatch(store etcd.Store) {
	onCreate := func(ctx context.Context, key []byte, value []byte) {
		log.Infof("Key %s has been created, value: %s", string(key), string(value))
	}
	err := store.Watch(context.TODO(), "ddd", onCreate, nil, nil)
	if err != nil {
		log.Fatal(err.Error())
	}
}
