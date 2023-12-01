// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package redis

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash"
	"strings"

	"github.com/buger/jsonparser"
)

// B64JSONPrefix stand for `{"` in base64.
const B64JSONPrefix = "ey"

// TokenHashAlgo ...
func TokenHashAlgo(token string) string {
	// Legacy tokens not b64 and not JSON records
	if strings.HasPrefix(token, B64JSONPrefix) {
		if jsonToken, err := base64.StdEncoding.DecodeString(token); err == nil {
			hashAlgo, _ := jsonparser.GetString(jsonToken, "h")

			return hashAlgo
		}
	}

	return ""
}

// TokenOrg ...
func TokenOrg(token string) string {
	if strings.HasPrefix(token, B64JSONPrefix) {
		if jsonToken, err := base64.StdEncoding.DecodeString(token); err == nil {
			// Checking error in case if it is a legacy tooken which just by accided has the same b64JSON prefix
			if org, err := jsonparser.GetString(jsonToken, "org"); err == nil {
				return org
			}
		}
	}

	// 24 is mongo bson id length
	if len(token) > 24 {
		return token[:24]
	}

	return ""
}

const (
	HashSha256 = "sha256"
)

func hashFunction(algorithm string) (hash.Hash, error) {
	switch algorithm {
	case HashSha256:
		return sha256.New(), nil
	default:
		return sha256.New(), fmt.Errorf("unknown key hash function: %s. Falling back to sha256", algorithm)
	}
}

// HashStr return hash the give string and return.
func HashStr(in string) string {
	h, _ := hashFunction(TokenHashAlgo(in))
	_, _ = h.Write([]byte(in))

	return hex.EncodeToString(h.Sum(nil))
}

// HashKey return hash the give string and return.
func HashKey(in string) string {
	return HashStr(in)
}
