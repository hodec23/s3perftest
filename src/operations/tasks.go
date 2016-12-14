// Package operations handle requests to server
package operations

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
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

// CoreTaskIntf interface
type CoreTaskIntf interface {
	Timeout() bool
	PutStats(n int)
	PutError(n int)
}

// CoreTask define core task
type CoreTask struct {
	ExpireTime time.Time
	StatsChan  chan<- int
	ErrorChan  chan<- int
}

// Timeout implements CoreTaskIntf
func (t *CoreTask) Timeout() bool {
	return time.Now().After(t.ExpireTime)
}

// put is to send number to channel
func put(c chan<- int, n int) {
	c <- n
}

// PutStats implements CoreTaskIntf
func (t *CoreTask) PutStats(n int) {
	put(t.StatsChan, n)
}

// PutError implements CoreTaskIntf
func (t *CoreTask) PutError(n int) {
	put(t.ErrorChan, n)
}

// Task interface
type Task interface {
	CoreTaskIntf
	Run(s *S3BucketOperations) error
}

// CreateObjectTask defines struct for create object task
type CreateObjectTask struct {
	CoreTask
	Key string
}

// Run implements Task interface
func (t *CreateObjectTask) Run(s *S3BucketOperations) error {
	err := s.CreateEmptyObject(t.Key)
	if err == nil {
		t.PutStats(1)
	} else if _, ok := err.(awserr.Error); ok {
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			if reqErr.StatusCode() == 500 {
				t.PutError(1)
			}
		}
	}
	return err
}

// ListObjectsTask defines struct for list objects task
type ListObjectsTask struct {
	CoreTask
	Prefix    string
	Delimiter string
	Marker    string
	MaxKeys   int64
	EndMarker string
}

// Run implements Task interface
func (t *ListObjectsTask) Run(s *S3BucketOperations) error {
	nextMarker := t.Marker
	for isTruncated := true; isTruncated; {
		//log.Printf("send request: %s", nextMarker)
		listObjectsOutput, err := s.listObjects(t.Prefix, t.Delimiter, nextMarker, t.MaxKeys)
		//log.Printf("got response: %s", nextMarker)
		if err != nil {
			log.Println(err)
			return err
		}
		if listObjectsOutput.NextMarker != nil {
			nextMarker = *listObjectsOutput.NextMarker
		} else if len(listObjectsOutput.Contents) > 0 {
			nextMarker = *listObjectsOutput.Contents[len(listObjectsOutput.Contents)-1].Key

		}
		for _, v := range listObjectsOutput.CommonPrefixes {
			log.Printf("CommonPrefix: %s", *v.Prefix)
		}
		//log.Printf("## NextMarker: %s", nextMarker)
		isTruncated = *listObjectsOutput.IsTruncated
		t.PutStats(len(listObjectsOutput.Contents) + len(listObjectsOutput.CommonPrefixes))

		if len(t.EndMarker) > 0 && nextMarker >= t.EndMarker {
			break
		}
		if t.Timeout() {
			log.Println("timeout")
			break
		}
	}
	return nil
}
