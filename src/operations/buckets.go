// Package operations handle requests to server
package operations

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
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

// S3BucketOperations defines basic structure for bucket operations
type S3BucketOperations struct {
	S3Client   *s3.S3
	BucketName string
}

// CreateBucket creates bucket if it doesn't exist; return ok if
func (s *S3BucketOperations) CreateBucket() error {
	_, err := s.S3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(s.BucketName),
	})
	if err != nil {
		if s3err, ok := err.(awserr.Error); ok {
			if s3err.Code() == "BucketAlreadyOwnedByYou" {
				return nil
			}
		}
		return err
	}

	return nil
}

// CreateEmptyObject creates empty object
func (s *S3BucketOperations) CreateEmptyObject(key string) error {
	_, err := s.S3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Println(err)
	}
	return err
}

// listObjects lists objects in one round
func (s *S3BucketOperations) listObjects(prefix, delimiter, marker string, maxkeys int64) (*s3.ListObjectsOutput, error) {
	// List Object Input Params
	listObjectInput := &s3.ListObjectsInput{
		Bucket: aws.String(s.BucketName),
	}
	if len(prefix) > 0 {
		listObjectInput.SetPrefix(prefix)
	}
	if len(delimiter) > 0 {
		listObjectInput.SetDelimiter(delimiter)
	}
	if len(marker) > 0 {
		listObjectInput.SetMarker(marker)
	}
	if maxkeys > 0 {
		listObjectInput.SetMaxKeys(maxkeys)
	}
	// List Objects
	return s.S3Client.ListObjects(listObjectInput)
}
