// Copyright 2025 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package log

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestDebugLogger(t *testing.T) {
	err := InitLogger(true)
	assert.NoError(t, err)

	Debug("Debug")
	Debugf("Debug for %s", "test")

	Errorw("Error", "err", errors.New("test"))
	Errorf("Error: %v", errors.New("test"))
}

func TestProdLogger(t *testing.T) {
	err := InitLogger(false)
	assert.NoError(t, err)

	Debug("Debug")
	Debugf("Debug for %s", "test")
	Infow("Info", "data", "test")
	Infof("Info: %v", "test")

	Errorw("Error", "err", errors.New("test"))
	Errorf("Error: %v", errors.New("test"))
}

func TestDefaultLogger(t *testing.T) {
	err := InitLogger(false)
	assert.NoError(t, err)

	SugarLogger().Info("This is a info message")

	logger := With("key0", "value0")
	logger.Info("This is a info message")
	logger.Infow("This is a info message", "key1", "value1")
}

func TestWithValues(t *testing.T) {
	_ = InitLogger(false)

	ctx := context.Background()
	ctx = WithContext(ctx)
	From(ctx).Info("this is a info message for ctx")
	ctx0 := WithContext(ctx, "k0", "v0")
	From(ctx0).Info("this is a info message for ctx0")
	ctx1 := WithContext(ctx0, "k1", "v1")
	From(ctx1).Info("this is a info message for ctx1")
	ctx2 := WithContext(ctx0, "k2", "v2")
	From(ctx2).Info("this is a info message for ctx2")
	ctx3 := WithContext(ctx1, "k3", "v3")
	From(ctx3).Info("this is a info message for ctx3")
	From(ctx1).Info("this is a info message for ctx1")
}
