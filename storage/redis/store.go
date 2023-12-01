// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package redis

import (
	goerrors "errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/spf13/viper"

	"github.com/wangtaoking1/app-base/errors"
	"github.com/wangtaoking1/app-base/log"
)

// ErrKeyNotFound is a standard error for when a key is not found in the storage engine.
var ErrKeyNotFound = errors.New("key not found")

// Store is the store interface of redis engine.
type Store interface {
	GetKey(string) (string, error) // Returned string is expected to be a JSON object (user.SessionState)
	GetMultiKey([]string) ([]string, error)
	GetRawKey(string) (string, error)
	SetKey(
		string,
		string,
		time.Duration,
	) error // Second input string is expected to be a JSON object (user.SessionState)
	SetRawKey(string, string, time.Duration) error
	SetExp(string, time.Duration) error   // Set key expiration
	GetExp(string) (time.Duration, error) // Returns expiry of a key
	GetKeys(string) []string
	DeleteKey(string) bool
	DeleteAllKeys() bool
	DeleteRawKey(string) bool
	GetKeysAndValues() map[string]string
	GetKeysAndValuesWithFilter(string) map[string]string
	DeleteKeys([]string) bool
	Decrement(string)
	IncrememntWithExpire(string, time.Duration) int64
	SetRollingWindow(key string, per int64, val string, pipeline bool) (int, []interface{})
	GetRollingWindow(key string, per int64, pipeline bool) (int, []interface{})
	GetSet(string) (map[string]string, error)
	AddToSet(string, string)
	GetAndDeleteSet(string) []interface{}
	RemoveFromSet(string, string)
	DeleteScanMatch(string) bool
	GetKeyPrefix() string
	AddToSortedSet(string, string, float64)
	GetSortedSetRange(string, string, string) ([]string, []float64, error)
	RemoveSortedSetRange(string, string, string) error
	GetListRange(string, int64, int64) ([]string, error)
	RemoveFromList(string, string) error
	AppendToSet(string, string)
	Exists(string) (bool, error)
}

type store struct {
	keyPrefix string
	keyHash   bool

	cli redis.UniversalClient
}

// Option config redis store.
type Option func(*store)

// WithKeyPrefix sets the key-prefix.
func WithKeyPrefix(prefix string) Option {
	return func(s *store) {
		s.keyPrefix = prefix
	}
}

// WithKeyHash enables key hashing.
func WithKeyHash() Option {
	return func(s *store) {
		s.keyHash = true
	}
}

// NewStore returns a new redis store with the global singleton redis client.
func NewStore() Store {
	if singletonInstance == nil {
		log.Fatal("Must init redis instance by InitInstance first before creating store")
	}

	return &store{
		cli: singletonInstance,
	}
}

func (s *store) hashKey(in string) string {
	if !s.keyHash {
		// Return raw key if no need hash
		return in
	}

	return HashStr(in)
}

func (s *store) fixKey(keyName string) string {
	return s.keyPrefix + s.hashKey(keyName)
}

func (s *store) cleanKey(keyName string) string {
	return strings.Replace(keyName, s.keyPrefix, "", 1)
}

// GetKey will retrieve a key from the database.
func (s *store) GetKey(keyName string) (string, error) {
	value, err := s.cli.Get(s.fixKey(keyName)).Result()
	if err != nil {
		log.Debugf("Error trying to get value: %s", err.Error())

		return "", ErrKeyNotFound
	}

	return value, nil
}

// GetMultiKey gets multiple keys from the database.
func (s *store) GetMultiKey(keys []string) ([]string, error) {
	keyNames := make([]string, len(keys))
	copy(keyNames, keys)
	for index, val := range keyNames {
		keyNames[index] = s.fixKey(val)
	}

	result := make([]string, 0)

	switch v := s.cli.(type) {
	case *redis.ClusterClient:
		{
			getCmds := make([]*redis.StringCmd, 0)
			pipe := v.Pipeline()
			for _, key := range keyNames {
				getCmds = append(getCmds, pipe.Get(key))
			}
			_, err := pipe.Exec()
			if err != nil && !goerrors.Is(err, redis.Nil) {
				log.Debugf("Error trying to get value: %s", err.Error())

				return nil, ErrKeyNotFound
			}
			for _, cmd := range getCmds {
				result = append(result, cmd.Val())
			}
		}
	case *redis.Client:
		{
			values, err := s.cli.MGet(keyNames...).Result()
			if err != nil {
				log.Debugf("Error trying to get value: %s", err.Error())

				return nil, ErrKeyNotFound
			}
			for _, val := range values {
				strVal := fmt.Sprint(val)
				if strVal == "<nil>" {
					strVal = ""
				}
				result = append(result, strVal)
			}
		}
	}

	for _, val := range result {
		if val != "" {
			return result, nil
		}
	}

	return nil, ErrKeyNotFound
}

// GetKeyTTL return ttl of the given key.
func (s *store) GetKeyTTL(keyName string) (ttl int64, err error) {
	duration, err := s.cli.TTL(s.fixKey(keyName)).Result()

	return int64(duration.Seconds()), err
}

// GetRawKey return the value of the given key.
func (s *store) GetRawKey(keyName string) (string, error) {
	value, err := s.cli.Get(keyName).Result()
	if err != nil {
		log.Debugf("Error trying to get value: %s", err.Error())

		return "", ErrKeyNotFound
	}

	return value, nil
}

// GetExp return the expiry of the given key.
func (s *store) GetExp(keyName string) (time.Duration, error) {
	log.Debugf("Getting exp for key: %s", s.fixKey(keyName))
	value, err := s.cli.TTL(s.fixKey(keyName)).Result()
	if err != nil {
		log.Errorf("Error trying to get TTL: ", err.Error())

		return 0, ErrKeyNotFound
	}

	return value, nil
}

// SetExp set expiry of the given key.
func (s *store) SetExp(keyName string, timeout time.Duration) error {
	err := s.cli.Expire(s.fixKey(keyName), timeout).Err()
	if err != nil {
		log.Errorf("Could not EXPIRE key: %s", err.Error())
	}

	return err
}

// SetKey will create (or update) a key value in the store.
func (s *store) SetKey(keyName, session string, timeout time.Duration) error {
	log.Debugf("[STORE] SET Raw key is: %s", keyName)
	log.Debugf("[STORE] Setting key: %s", s.fixKey(keyName))

	err := s.cli.Set(s.fixKey(keyName), session, timeout).Err()
	if err != nil {
		log.Errorf("Error trying to set value: %s", err.Error())

		return err
	}

	return nil
}

// SetRawKey set the value of the given key.
func (s *store) SetRawKey(keyName, session string, timeout time.Duration) error {
	err := s.cli.Set(keyName, session, timeout).Err()
	if err != nil {
		log.Errorf("Error trying to set value: %s", err.Error())

		return err
	}

	return nil
}

// Decrement will decrement a key in redis.
func (s *store) Decrement(keyName string) {
	keyName = s.fixKey(keyName)
	log.Debugf("Decrementing key: %s", keyName)
	err := s.cli.Decr(keyName).Err()
	if err != nil {
		log.Errorf("Error trying to decrement value: %s", err.Error())
	}
}

// IncrememntWithExpire will increment a key in redis.
func (s *store) IncrememntWithExpire(keyName string, expire time.Duration) int64 {
	log.Debugf("Incrementing raw key: %s", keyName)
	// This function uses a raw key, so we shouldn't call fixKey
	fixedKey := keyName
	val, err := s.cli.Incr(fixedKey).Result()

	if err != nil {
		log.Errorf("Error trying to increment value: %s", err.Error())
	} else {
		log.Debugf("Incremented key: %s, val is: %d", fixedKey, val)
	}

	if val == 1 && expire > 0 {
		log.Debug("--> Setting Expire")
		s.cli.Expire(fixedKey, expire)
	}

	return val
}

// GetKeys will return all keys according to the filter (filter is a prefix - e.g. tyk.keys.*).
func (s *store) GetKeys(filter string) []string {
	filterHash := ""
	if filter != "" {
		filterHash = s.hashKey(filter)
	}
	searchStr := s.keyPrefix + filterHash + "*"
	log.Debugf("[STORE] Getting list by: %s", searchStr)

	fnFetchKeys := func(client *redis.Client) ([]string, error) {
		values := make([]string, 0)

		iter := client.Scan(0, searchStr, 0).Iterator()
		for iter.Next() {
			values = append(values, iter.Val())
		}

		if err := iter.Err(); err != nil {
			return nil, err
		}

		return values, nil
	}

	var err error
	var values []string
	sessions := make([]string, 0)

	switch v := s.cli.(type) {
	case *redis.ClusterClient:
		ch := make(chan []string)

		go func() {
			err = v.ForEachMaster(func(client *redis.Client) error {
				values, err = fnFetchKeys(client)
				if err != nil {
					return err
				}

				ch <- values

				return nil
			})
			close(ch)
		}()

		for res := range ch {
			sessions = append(sessions, res...)
		}
	case *redis.Client:
		sessions, err = fnFetchKeys(v)
	}

	if err != nil {
		log.Errorf("Error while fetching keys: %s", err)

		return nil
	}

	for i, v := range sessions {
		sessions[i] = s.cleanKey(v)
	}

	return sessions
}

// GetKeysAndValuesWithFilter will return all keys and their values with a filter.
func (s *store) GetKeysAndValuesWithFilter(filter string) map[string]string {
	keys := s.GetKeys(filter)
	if keys == nil {
		log.Error("Error trying to get filtered client keys")

		return nil
	}

	if len(keys) == 0 {
		return nil
	}

	for i, v := range keys {
		keys[i] = s.keyPrefix + v
	}

	client := s.cli
	values := make([]string, 0)

	switch v := client.(type) {
	case *redis.ClusterClient:
		{
			getCmds := make([]*redis.StringCmd, 0)
			pipe := v.Pipeline()
			for _, key := range keys {
				getCmds = append(getCmds, pipe.Get(key))
			}
			_, err := pipe.Exec()
			if err != nil && !goerrors.Is(err, redis.Nil) {
				log.Errorf("Error trying to get client keys: %s", err.Error())

				return nil
			}

			for _, cmd := range getCmds {
				values = append(values, cmd.Val())
			}
		}
	case *redis.Client:
		{
			result, err := v.MGet(keys...).Result()
			if err != nil {
				log.Errorf("Error trying to get client keys: %s", err.Error())

				return nil
			}

			for _, val := range result {
				strVal := fmt.Sprint(val)
				if strVal == "<nil>" {
					strVal = ""
				}
				values = append(values, strVal)
			}
		}
	}

	m := make(map[string]string)
	for i, v := range keys {
		m[s.cleanKey(v)] = values[i]
	}

	return m
}

// GetKeysAndValues will return all keys and their values - not to be used lightly.
func (s *store) GetKeysAndValues() map[string]string {
	return s.GetKeysAndValuesWithFilter("")
}

// DeleteKey will remove a key from the database.
func (s *store) DeleteKey(keyName string) bool {
	log.Debugf("DEL Key was: %s", keyName)
	log.Debugf("DEL Key became: %s", s.fixKey(keyName))
	n, err := s.cli.Del(s.fixKey(keyName)).Result()
	if err != nil {
		log.Errorf("Error trying to delete key: %s", err.Error())
	}

	return n > 0
}

// DeleteAllKeys will remove all keys from the database.
func (s *store) DeleteAllKeys() bool {
	n, err := s.cli.FlushAll().Result()
	if err != nil {
		log.Errorf("Error trying to delete keys: %s", err.Error())
	}

	if n == "OK" {
		return true
	}

	return false
}

// DeleteRawKey will remove a key from the database without prefixing, assumes user knows what they are doing.
func (s *store) DeleteRawKey(keyName string) bool {
	n, err := s.cli.Del(keyName).Result()
	if err != nil {
		log.Errorf("Error trying to delete key: %s", err.Error())
	}

	return n > 0
}

// DeleteScanMatch will remove a group of keys in bulk.
func (s *store) DeleteScanMatch(pattern string) bool {
	client := s.cli
	log.Debugf("Deleting: %s", pattern)

	fnScan := func(client *redis.Client) ([]string, error) {
		values := make([]string, 0)

		iter := client.Scan(0, pattern, 0).Iterator()
		for iter.Next() {
			values = append(values, iter.Val())
		}

		if err := iter.Err(); err != nil {
			return nil, err
		}

		return values, nil
	}

	var err error
	var keys []string
	var values []string

	switch v := client.(type) {
	case *redis.ClusterClient:
		ch := make(chan []string)
		go func() {
			err = v.ForEachMaster(func(client *redis.Client) error {
				values, err = fnScan(client)
				if err != nil {
					return err
				}

				ch <- values

				return nil
			})
			close(ch)
		}()

		for vals := range ch {
			keys = append(keys, vals...)
		}
	case *redis.Client:
		keys, err = fnScan(v)
	}

	if err != nil {
		log.Errorf("SCAN command field with err: %s", err.Error())

		return false
	}

	if len(keys) > 0 {
		for _, name := range keys {
			log.Infof("Deleting: %s", name)
			err := client.Del(name).Err()
			if err != nil {
				log.Errorf("Error trying to delete key: %s - %s", name, err.Error())
			}
		}
		log.Infof("Deleted: %d records", len(keys))
	} else {
		log.Debug("RedisCluster called DEL - Nothing to delete")
	}

	return true
}

// DeleteKeys will remove a group of keys in bulk.
func (s *store) DeleteKeys(keys []string) bool {
	if len(keys) > 0 {
		for i, v := range keys {
			keys[i] = s.fixKey(v)
		}

		log.Debugf("Deleting: %v", keys)
		client := s.cli
		switch v := client.(type) {
		case *redis.ClusterClient:
			{
				pipe := v.Pipeline()
				for _, k := range keys {
					pipe.Del(k)
				}

				if _, err := pipe.Exec(); err != nil {
					log.Errorf("Error trying to delete keys: %s", err.Error())
				}
			}
		case *redis.Client:
			{
				_, err := v.Del(keys...).Result()
				if err != nil {
					log.Errorf("Error trying to delete keys: %s", err.Error())
				}
			}
		}
	} else {
		log.Debug("RedisCluster called DEL - Nothing to delete")
	}

	return true
}

// StartPubSubHandler will listen for a signal and run the callback for
// every subscription and message event.
func (s *store) StartPubSubHandler(channel string, callback func(interface{})) error {
	pubsub := s.cli.Subscribe(channel)
	defer pubsub.Close()

	if _, err := pubsub.Receive(); err != nil {
		log.Errorf("Error while receiving pubsub message: %s", err.Error())

		return err
	}

	for msg := range pubsub.Channel() {
		callback(msg)
	}

	return nil
}

// Publish publishes a message to the specify channel.
func (s *store) Publish(channel, message string) error {
	err := s.cli.Publish(channel, message).Err()
	if err != nil {
		log.Errorf("Error trying to set value: %s", err.Error())

		return err
	}

	return nil
}

// GetAndDeleteSet get and delete a key.
func (s *store) GetAndDeleteSet(keyName string) []interface{} {
	log.Debugf("Getting raw key set: %s", keyName)
	log.Debugf("keyName is: %s", keyName)
	fixedKey := s.fixKey(keyName)
	log.Debugf("Fixed keyname is: %s", fixedKey)

	client := s.cli

	var lrange *redis.StringSliceCmd
	_, err := client.TxPipelined(func(pipe redis.Pipeliner) error {
		lrange = pipe.LRange(fixedKey, 0, -1)
		pipe.Del(fixedKey)

		return nil
	})
	if err != nil {
		log.Errorf("Multi command failed: %s", err.Error())

		return nil
	}

	vals := lrange.Val()
	log.Debugf("Analytics returned: %d", len(vals))
	if len(vals) == 0 {
		return nil
	}

	log.Debugf("Unpacked vals: %d", len(vals))
	result := make([]interface{}, len(vals))
	for i, v := range vals {
		result[i] = v
	}

	return result
}

// AppendToSet append a value to the key set.
func (s *store) AppendToSet(keyName, value string) {
	fixedKey := s.fixKey(keyName)
	log.Debug("Pushing to raw key list", "keyName", keyName)
	log.Debug("Appending to fixed key list", "fixedKey", fixedKey)
	if err := s.cli.RPush(fixedKey, value).Err(); err != nil {
		log.Errorf("Error trying to append to set keys: %s", err.Error())
	}
}

// Exists check if keyName exists.
func (s *store) Exists(keyName string) (bool, error) {
	fixedKey := s.fixKey(keyName)
	log.Debug("Checking if exists", "keyName", fixedKey)

	exists, err := s.cli.Exists(fixedKey).Result()
	if err != nil {
		log.Errorf("Error trying to check if key exists: %s", err.Error())

		return false, err
	}
	if exists == 1 {
		return true, nil
	}

	return false, nil
}

// RemoveFromList delete an value from a list idetinfied with the keyName.
func (s *store) RemoveFromList(keyName, value string) error {
	fixedKey := s.fixKey(keyName)

	log.Debug("Removing value from list", "keyName", keyName, "fixedKey", fixedKey, "value", value)

	if err := s.cli.LRem(fixedKey, 0, value).Err(); err != nil {
		log.Error("LREM command failed", "keyName", keyName, "fixedKey", fixedKey,
			"value", value, "error", err.Error())

		return err
	}

	return nil
}

// GetListRange gets range of elements of list identified by keyName.
func (s *store) GetListRange(keyName string, from, to int64) ([]string, error) {
	fixedKey := s.fixKey(keyName)

	elements, err := s.cli.LRange(fixedKey, from, to).Result()
	if err != nil {
		log.Error("LRANGE command failed", "keyName", keyName, "fixedKey", fixedKey,
			"from", from, "to", to, "error", err.Error())

		return nil, err
	}

	return elements, nil
}

// AppendToSetPipelined append values to redis pipeline.
func (s *store) AppendToSetPipelined(key string, values [][]byte) {
	if len(values) == 0 {
		return
	}

	pipe := s.cli.Pipeline()
	for _, val := range values {
		pipe.RPush(s.fixKey(key), val)
	}

	if _, err := pipe.Exec(); err != nil {
		log.Errorf("Error trying to append to set keys: %s", err.Error())
	}

	// if we need to set an expiration time
	if storageExpTime := int64(viper.GetDuration("analytics.storage-expiration-time")); storageExpTime != int64(-1) {
		// If there is no expiry on the analytics set, we should set it.
		exp, _ := s.GetExp(key)
		if exp == -1 {
			_ = s.SetExp(key, time.Duration(storageExpTime)*time.Second)
		}
	}
}

// GetSet return key set value.
func (s *store) GetSet(keyName string) (map[string]string, error) {
	log.Debugf("Getting from key set: %s", keyName)
	log.Debugf("Getting from fixed key set: %s", s.fixKey(keyName))
	val, err := s.cli.SMembers(s.fixKey(keyName)).Result()
	if err != nil {
		log.Errorf("Error trying to get key set: %s", err.Error())

		return nil, err
	}

	result := make(map[string]string)
	for i, value := range val {
		result[strconv.Itoa(i)] = value
	}

	return result, nil
}

// AddToSet add value to key set.
func (s *store) AddToSet(keyName, value string) {
	log.Debugf("Pushing to raw key set: %s", keyName)
	log.Debugf("Pushing to fixed key set: %s", s.fixKey(keyName))
	err := s.cli.SAdd(s.fixKey(keyName), value).Err()
	if err != nil {
		log.Errorf("Error trying to append keys: %s", err.Error())
	}
}

// RemoveFromSet remove a value from key set.
func (s *store) RemoveFromSet(keyName, value string) {
	log.Debugf("Removing from raw key set: %s", keyName)
	log.Debugf("Removing from fixed key set: %s", s.fixKey(keyName))
	err := s.cli.SRem(s.fixKey(keyName), value).Err()
	if err != nil {
		log.Errorf("Error trying to remove keys: %s", err.Error())
	}
}

// IsMemberOfSet return whether the given value belong to key set.
func (s *store) IsMemberOfSet(keyName, value string) bool {
	val, err := s.cli.SIsMember(s.fixKey(keyName), value).Result()
	if err != nil {
		log.Errorf("Error trying to check set member: %s", err.Error())

		return false
	}

	log.Debugf("SISMEMBER %s %s %v %v", keyName, value, val, err)

	return val
}

// SetRollingWindow will append to a sorted set in redis and extract a timed window of values.
func (s *store) SetRollingWindow(keyName string, per int64, valueOverride string, pipeline bool) (int, []interface{}) {
	log.Debugf("Incrementing raw key: %s", keyName)
	log.Debugf("keyName is: %s", keyName)
	now := time.Now()
	log.Debugf("Now is: %v", now)
	onePeriodAgo := now.Add(time.Duration(-1*per) * time.Second)
	log.Debugf("Then is: %v", onePeriodAgo)

	client := s.cli
	var zrange *redis.StringSliceCmd

	pipeFn := func(pipe redis.Pipeliner) error {
		pipe.ZRemRangeByScore(keyName, "-inf", strconv.Itoa(int(onePeriodAgo.UnixNano())))
		zrange = pipe.ZRange(keyName, 0, -1)

		element := redis.Z{
			Score: float64(now.UnixNano()),
		}

		if valueOverride != "-1" {
			element.Member = valueOverride
		} else {
			element.Member = strconv.Itoa(int(now.UnixNano()))
		}

		pipe.ZAdd(keyName, &element)
		pipe.Expire(keyName, time.Duration(per)*time.Second)

		return nil
	}

	var err error
	if pipeline {
		_, err = client.Pipelined(pipeFn)
	} else {
		_, err = client.TxPipelined(pipeFn)
	}

	if err != nil {
		log.Errorf("Multi command failed: %s", err.Error())

		return 0, nil
	}

	values := zrange.Val()

	// Check actual value
	if values == nil {
		return 0, nil
	}

	intVal := len(values)
	result := make([]interface{}, len(values))

	for i, v := range values {
		result[i] = v
	}

	log.Debugf("Returned: %d", intVal)

	return intVal, result
}

// GetRollingWindow return rolling window.
func (s *store) GetRollingWindow(keyName string, per int64, pipeline bool) (int, []interface{}) {
	now := time.Now()
	onePeriodAgo := now.Add(time.Duration(-1*per) * time.Second)

	client := s.cli
	var zrange *redis.StringSliceCmd

	pipeFn := func(pipe redis.Pipeliner) error {
		pipe.ZRemRangeByScore(keyName, "-inf", strconv.Itoa(int(onePeriodAgo.UnixNano())))
		zrange = pipe.ZRange(keyName, 0, -1)

		return nil
	}

	var err error
	if pipeline {
		_, err = client.Pipelined(pipeFn)
	} else {
		_, err = client.TxPipelined(pipeFn)
	}
	if err != nil {
		log.Errorf("Multi command failed: %s", err.Error())

		return 0, nil
	}

	values := zrange.Val()

	// Check actual value
	if values == nil {
		return 0, nil
	}

	intVal := len(values)
	result := make([]interface{}, intVal)
	for i, v := range values {
		result[i] = v
	}

	log.Debugf("Returned: %d", intVal)

	return intVal, result
}

// GetKeyPrefix returns storage key prefix.
func (s *store) GetKeyPrefix() string {
	return s.keyPrefix
}

// AddToSortedSet adds value with given score to sorted set identified by keyName.
func (s *store) AddToSortedSet(keyName, value string, score float64) {
	fixedKey := s.fixKey(keyName)

	log.Debug("Pushing raw key to sorted set", "keyName", keyName, "fixedKey", fixedKey)

	member := redis.Z{Score: score, Member: value}
	if err := s.cli.ZAdd(fixedKey, &member).Err(); err != nil {
		log.Error("ZADD command failed", "keyName", keyName, "fixedKey", fixedKey,
			"error", err.Error())
	}
}

// GetSortedSetRange gets range of elements of sorted set identified by keyName.
func (s *store) GetSortedSetRange(keyName, scoreFrom, scoreTo string) ([]string, []float64, error) {
	fixedKey := s.fixKey(keyName)
	log.Debug("Getting sorted set range", "keyName", keyName, "fixedKey", fixedKey,
		"scoreFrom", scoreFrom, "scoreTo", scoreTo)

	args := redis.ZRangeBy{Min: scoreFrom, Max: scoreTo}
	values, err := s.cli.ZRangeByScoreWithScores(fixedKey, &args).Result()
	if err != nil {
		log.Error("ZRANGEBYSCORE command failed", "keyName", keyName, "fixedKey", fixedKey,
			"scoreFrom", scoreFrom, "scoreTo", scoreTo, "error", err.Error())

		return nil, nil, err
	}

	if len(values) == 0 {
		return nil, nil, nil
	}

	elements := make([]string, len(values))
	scores := make([]float64, len(values))

	for i, v := range values {
		elements[i] = fmt.Sprint(v.Member)
		scores[i] = v.Score
	}

	return elements, scores, nil
}

// RemoveSortedSetRange removes range of elements from sorted set identified by keyName.
func (s *store) RemoveSortedSetRange(keyName, scoreFrom, scoreTo string) error {
	fixedKey := s.fixKey(keyName)

	log.Debug("Removing sorted set range", "keyName", keyName, "fixedKey", fixedKey,
		"scoreFrom", scoreFrom, "scoreTo", scoreTo)

	if err := s.cli.ZRemRangeByScore(fixedKey, scoreFrom, scoreTo).Err(); err != nil {
		log.Debug("ZREMRANGEBYSCORE command failed", "keyName", keyName, "fixedKey", fixedKey,
			"scoreFrom", scoreFrom, "scoreTo", scoreTo, "error", err.Error())

		return err
	}

	return nil
}
