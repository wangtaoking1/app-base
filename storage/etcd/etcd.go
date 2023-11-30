// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package etcd

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/wangtaoking1/app-base/errors"
	"github.com/wangtaoking1/app-base/log"
	"github.com/wangtaoking1/app-base/utils/retry"
)

var defaultRetryInterval = 1 * time.Second

// Store defines the interface of etcd store.
type Store interface {
	RawClient() *clientv3.Client
	Put(ctx context.Context, key string, val string) error
	PutWithSession(ctx context.Context, key string, val string) error
	PutWithLease(ctx context.Context, key string, val string, ttlSeconds int64) error
	Get(ctx context.Context, key string) ([]byte, error)
	List(ctx context.Context, prefix string) ([]KeyValue, error)
	Delete(ctx context.Context, key string) ([]byte, error)
	Watch(ctx context.Context, prefix string,
		onCreate CreateEventFunc, onUpdate UpdateEventFunc, onDelete DeleteEventFunc) error
	Unwatch(prefix string)
	Close() error
}

type store struct {
	options *Options

	cli *clientv3.Client

	// leaseID is for keeping session. When the client store closed,
	// the lease session will be removed automatically.
	leaseID      clientv3.LeaseID
	leaseKeeping bool

	watchers map[string]*watcher
	mtx      sync.Mutex
}

// New creates a new etcd client store.
func New(opts *Options) Store {
	cli, err := initClient(opts)
	if err != nil {
		log.Fatalf("Failed to init etcd client: %v", err)
	}

	s := &store{
		options:  opts,
		cli:      cli,
		watchers: make(map[string]*watcher),
	}

	if err = s.startSession(); err != nil {
		log.Fatalf("Failed to start etcd session: %v", err)
	}

	return s
}

func initClient(opts *Options) (*clientv3.Client, error) {
	tlsConfig, err := opts.loadTLSConfig()
	if err != nil {
		return nil, err
	}

	cfg := clientv3.Config{
		Endpoints:   opts.Endpoints,
		DialTimeout: opts.Timeout,
		Username:    opts.Username,
		Password:    opts.Password,
		TLS:         tlsConfig,

		DialOptions: []grpc.DialOption{
			grpc.WithBlock(),
		},
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}

	return cli, nil
}

func (s *store) startSession() error {
	ctx := context.TODO()

	resp, err := s.cli.Grant(ctx, int64(s.options.LeaseExpire.Seconds()))
	if err != nil {
		return errors.Wrap(err, "create new lease error")
	}
	s.leaseID = resp.ID

	ch, err := s.cli.KeepAlive(ctx, s.leaseID)
	if err != nil {
		return errors.Wrap(err, "keep alive failed")
	}
	s.leaseKeeping = true

	go func() {
		for {
			if _, ok := <-ch; !ok {
				s.leaseKeeping = false
				s.onKeepaliveFailure()

				break
			}
		}
	}()

	return nil
}

func (s *store) onKeepaliveFailure() {
	if s.leaseKeeping {
		// No need handle failure.
		return
	}

	log.Error("Keep alive etcd session error, try restart...")
	err := retry.RetryWithTimeout(context.TODO(), defaultRetryInterval, 3*defaultRetryInterval, func() error {
		e := s.startSession()
		if e != nil {
			return errors.Wrapf(retry.RetryableErr, "restart session error: %v", e)
		}

		return nil
	})
	if err != nil {
		log.Fatalf("Restart etcd session failed: %v", err)
	}
	log.Info("Etcd session has been restart")
}

func (s *store) RawClient() *clientv3.Client {
	return s.cli
}

func (s *store) getKey(key string) string {
	if len(s.options.Namespace) == 0 {
		return key
	}

	return fmt.Sprintf("/%s/%s", s.options.Namespace, key)
}

func (s *store) put(ctx context.Context, key string, val string, session bool) error {
	nctx, cancel := context.WithTimeout(ctx, s.options.RequestTimeout)
	defer cancel()

	key = s.getKey(key)
	if session {
		if _, err := s.cli.Put(nctx, key, val, clientv3.WithLease(s.leaseID)); err != nil {
			return errors.Wrap(err, "put key-value pair to etcd failed")
		}

		return nil
	}

	if _, err := s.cli.Put(nctx, key, val); err != nil {
		return errors.Wrap(err, "put key-value pair to etcd failed")
	}

	return nil
}

func (s *store) Put(ctx context.Context, key string, val string) error {
	return s.put(ctx, key, val, false)
}

func (s *store) PutWithSession(ctx context.Context, key string, val string) error {
	return s.put(ctx, key, val, true)
}

func (s *store) grantLease(ctx context.Context, ttlSeconds int64) (*clientv3.LeaseGrantResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.options.RequestTimeout)
	defer cancel()
	resp, err := s.cli.Grant(ctx, ttlSeconds)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *store) PutWithLease(ctx context.Context, key string, val string, ttlSeconds int64) error {
	resp, err := s.grantLease(ctx, ttlSeconds)
	if err != nil {
		return errors.Wrap(err, "grant lease error")
	}

	ctx, cancel := context.WithTimeout(ctx, s.options.RequestTimeout)
	defer cancel()

	key = s.getKey(key)
	if _, err = s.cli.Put(ctx, key, val, clientv3.WithLease(resp.ID)); err != nil {
		return errors.Wrap(err, "put key-value pair to etcd failed")
	}

	return nil
}

func (s *store) Get(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, s.options.RequestTimeout)
	defer cancel()

	key = s.getKey(key)
	log.Infof("key is %s", key)

	resp, err := s.cli.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "get key from etcd failed")
	}
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("key %s not found", key)
	}

	return resp.Kvs[0].Value, nil
}

// KeyValue defines etcd returned key-value pairs.
type KeyValue struct {
	Key   string
	Value []byte
}

func (s *store) List(ctx context.Context, prefix string) ([]KeyValue, error) {
	ctx, cancel := context.WithTimeout(ctx, s.options.RequestTimeout)
	defer cancel()

	prefix = s.getKey(prefix)

	resp, err := s.cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Wrap(err, "list keys from etcd failed")
	}
	ret := make([]KeyValue, len(resp.Kvs))
	sidx := len(s.options.Namespace) + 2
	for i := 0; i < len(resp.Kvs); i++ {
		ret[i] = KeyValue{
			Key:   string(resp.Kvs[i].Key[sidx:]),
			Value: resp.Kvs[i].Value,
		}
	}

	return ret, nil
}

func (s *store) Delete(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, s.options.RequestTimeout)
	defer cancel()

	key = s.getKey(key)

	dresp, err := s.cli.Delete(ctx, key, clientv3.WithPrevKV())
	if err != nil {
		return nil, errors.Wrap(err, "delete key from etcd failed")
	}

	if dresp.Deleted == 1 && len(dresp.PrevKvs) > 0 {
		return dresp.PrevKvs[0].Value, nil
	}

	return nil, nil
}

// CreateEventFunc defines etcd create event function handler.
type CreateEventFunc func(ctx context.Context, key, value []byte)

// UpdateEventFunc defines etcd update event function handler.
type UpdateEventFunc func(ctx context.Context, key, oldvalue, value []byte)

// DeleteEventFunc defines etcd delete event function handler.
type DeleteEventFunc func(ctx context.Context, key []byte)

// watcher defines a etcd watcher.
type watcher struct {
	clientv3.Watcher
	cancel context.CancelFunc

	prefix   string
	onCreate CreateEventFunc
	onUpdate UpdateEventFunc
	onDelete DeleteEventFunc
}

func (w *watcher) startWatch(ctx context.Context, namespace string) {
	sidx := len(namespace) + 2
	rch := w.Watch(ctx, w.prefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
	go func() {
		for wresp := range rch {
			for _, ev := range wresp.Events {
				key := ev.Kv.Key[sidx:]
				if ev.PrevKv == nil {
					if w.onCreate != nil {
						w.onCreate(ctx, key, ev.Kv.Value)
					}
				} else {
					switch ev.Type {
					case mvccpb.PUT:
						if w.onUpdate != nil {
							w.onUpdate(ctx, key, ev.PrevKv.Value, ev.Kv.Value)
						}
					case mvccpb.DELETE:
						if w.onDelete != nil {
							w.onDelete(ctx, key)
						}
					}
				}
			}
		}
		log.Infof("Watcher %s stopped", w.prefix)
	}()
}

func (w *watcher) close() {
	_ = w.Close()
	w.cancel()
}

func (s *store) getWatcher(prefix string) *watcher {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.watchers[prefix]
}

func (s *store) addWatcher(prefix string, w *watcher) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.watchers[prefix] = w
}

func (s *store) removeWatcher(prefix string) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	delete(s.watchers, prefix)
}

// Watch watches the keys in etcd.
func (s *store) Watch(ctx context.Context, prefix string,
	onCreate CreateEventFunc, onUpdate UpdateEventFunc, onDelete DeleteEventFunc,
) error {
	if s.getWatcher(prefix) != nil {
		return fmt.Errorf("watch prefix %s already registered", prefix)
	}

	ctx, cancel := context.WithCancel(ctx)
	w := &watcher{
		Watcher:  clientv3.NewWatcher(s.cli),
		cancel:   cancel,
		prefix:   s.getKey(prefix),
		onCreate: onCreate,
		onUpdate: onUpdate,
		onDelete: onDelete,
	}

	s.addWatcher(prefix, w)
	w.startWatch(ctx, s.options.Namespace)

	return nil
}

func (s *store) Unwatch(prefix string) {
	w := s.getWatcher(prefix)
	if w == nil {
		log.Debugf("Prefix %s not watched!!", prefix)

		return
	}
	w.close()
	s.removeWatcher(prefix)
	log.Debugf("Unwatch prefix %s", prefix)
}

func (s *store) Close() error {
	if s.cli == nil {
		return nil
	}

	return s.cli.Close()
}
