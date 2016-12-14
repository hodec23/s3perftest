package utils

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
)

/*
 * Copyright 2016 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

var s3LetterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!-_.*'()")

// GenRandS3Key generates random s3 key
func GenRandS3Key(keylen int) string {
	b := make([]rune, keylen)
	for i := range b {
		b[i] = s3LetterRunes[rand.Intn(len(s3LetterRunes))]
	}
	return string(b)
}

// GenNSortedS3Keys generates n sorted s3 keys
func GenNSortedS3Keys(n, keylen int) []string {
	if n <= 0 {
		return []string{}
	}
	result := make([]string, n)
	for i := 0; i < n; i++ {
		result[i] = GenRandS3Key(keylen)
	}
	sort.StringSlice(result).Sort()
	return result
}

// GenS3NamespaceKey generates s3 key in n levels
func GenS3NamespaceKey(keylen int, levelNamePrefix string, numOfSubDirs ...int) string {
	var buffer bytes.Buffer
	for _, n := range numOfSubDirs {
		buffer.WriteString(levelNamePrefix)
		// e.g. "%04d" --> 0256
		buffer.WriteString(fmt.Sprintf("%0"+strconv.Itoa(int(math.Log10(float64(n)))+1)+"d", rand.Intn(n)))
		buffer.WriteString("/")
	}
	buffer.WriteString(GenRandS3Key(keylen))
	return buffer.String()
}

// GenNSortedS3NamespaceKeys generates n sorted s3 namespace keys
func GenNSortedS3NamespaceKeys(n, keylen int, levelNamePrefix string, numOfSubDirs ...int) []string {
	if n <= 0 {
		return []string{}
	}
	result := make([]string, n)
	for i := 0; i < n; i++ {
		result[i] = GenS3NamespaceKey(keylen, levelNamePrefix, numOfSubDirs...)
	}
	sort.StringSlice(result).Sort()
	return result
}

// GenNSortedS3NamespaceKeysWithPrefix generates n sorted s3 namespace keys
func GenNSortedS3NamespaceKeysWithPrefix(keyPrefix string, n, keylen int, levelNamePrefix string, numOfSubDirs ...int) []string {
	tempKeys := GenNSortedS3NamespaceKeys(n, keylen, levelNamePrefix, numOfSubDirs...)
	result := make([]string, len(tempKeys))
	for idx, v := range tempKeys {
		if len(keyPrefix) > len(v) {
			return []string{}
		}
		result[idx] = keyPrefix + v[len(keyPrefix):]
	}
	sort.StringSlice(result).Sort()
	return result
}
