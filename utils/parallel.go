// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package utils

import (
	"context"

	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
)

// ParallelList 并行获取函数
// 并发执行查询，最后将查询结果汇总返回
func ParallelList[K, V any](ctx context.Context, paralLimit int, keys []K, fn func(ctx context.Context, key K) (V, error)) ([]V, error) {
	var eg errgroup.Group
	if paralLimit > 0 {
		eg.SetLimit(paralLimit)
	}

	results := make([]V, len(keys))
	for i, key := range keys {
		eg.Go(func() error {
			resItem, err := fn(ctx, key)
			if err != nil {
				return err
			}
			results[i] = resItem
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

// ParallelBatchList 并行获取函数
// 将任务分成若干个 chunks，然后并发执行查询，最后将查询结果汇总返回
func ParallelBatchList[K, V any](ctx context.Context, paralLimit, batchSize int, keys []K, fn func(ctx context.Context, keys []K) ([]V, error)) ([]V, error) {
	var eg errgroup.Group
	if paralLimit > 0 {
		eg.SetLimit(paralLimit)
	}

	chunks := lo.Chunk(keys, batchSize)
	resultSlice := make([][]V, len(chunks))
	for i, chunk := range chunks {
		eg.Go(func() error {
			resItems, err := fn(ctx, chunk)
			if err != nil {
				return err
			}
			resultSlice[i] = resItems
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return lo.Flatten(resultSlice), nil
}

// ParallelDo 并行执行函数
// 并发执行，返回是否报错
func ParallelDo[T any](ctx context.Context, paralLimit int, items []T, fn func(ctx context.Context, item T) error) error {
	var eg errgroup.Group
	if paralLimit > 0 {
		eg.SetLimit(paralLimit)
	}

	for _, item := range items {
		eg.Go(func() error {
			return fn(ctx, item)
		})
	}
	return eg.Wait()
}

// ParallelBatchDo 并行执行函数
// 将任务分成若干个 chunks，然后并发执行，返回是否报错
func ParallelBatchDo[T any](ctx context.Context, paralLimit, batchSize int, items []T, fn func(ctx context.Context, items []T) error) error {
	var eg errgroup.Group
	if paralLimit > 0 {
		eg.SetLimit(paralLimit)
	}

	chunks := lo.Chunk(items, batchSize)
	for _, chunk := range chunks {
		eg.Go(func() error {
			return fn(ctx, chunk)
		})
	}
	return eg.Wait()
}

// ParalExec 并行执行所有任务函数
// 并发执行，返回是否报错
func ParalExec[T any](ctx context.Context, fns ...func(ctx context.Context) error) error {
	var eg errgroup.Group

	for _, fn := range fns {
		if fn == nil {
			continue
		}

		eg.Go(func() error {
			return fn(ctx)
		})
	}
	return eg.Wait()
}

// ParallelByFirst 并行执行多个方法，返回第一个成功的结果
// 如果都失败，则返回最后一个错误
func ParallelByFirst[K, V any](ctx context.Context, key K, fns ...func(ctx context.Context, key K) (V, error)) (V, error) {
	var empty V
	if len(fns) == 0 {
		return empty, nil
	}

	var (
		resChan    = make(chan V)
		errChan    = make(chan error)
		errorCount int
	)
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
	}()
	for _, fn := range fns {
		go func() {
			res, err := fn(ctx, key)
			if err != nil {
				select {
				case errChan <- err:
				case <-ctx.Done():
				}
				return
			}
			select {
			case resChan <- res:
			case <-ctx.Done():
			}
		}()
	}

	for {
		select {
		case res := <-resChan:
			return res, nil
		case err := <-errChan:
			errorCount++
			if errorCount >= len(fns) {
				return empty, err
			}
		case <-ctx.Done():
			return empty, ctx.Err()
		}
	}
}
