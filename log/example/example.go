package main

import (
	"fmt"

	"github.com/wangtaoking1/app-base/log"
)

func testPanic() {
	defer func() {
		if r := recover(); r != nil {
			log.Error("panic has been recovered")
		}
	}()

	log.Panic("this is panic msg")
}

func main() {
	defer log.Flush()

	log.Debug("this is debug msg", "key", "val")
	log.Debugf("this is info msg, kv: %s=%s", "key", "val")
	log.V(log.DebugLevel).Info("this is debug msg", "key", "val")

	log.Info("this is info msg", "key", "val")
	log.Infof("this is info msg, kv: %s=%s", "key", "val")

	log.Warn("this is warn msg", "key", "val")
	log.Warnf("this is warn msg, kv: %s=%s", "key", "val")

	err := fmt.Errorf("this is an errors")
	log.Error("this is errors msg", "err", err)
	log.Errorf("this is errors msg, with err: %v", err)

	testPanic()

	log.Fatal("this is fatal msg")
}
