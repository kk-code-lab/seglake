package main

import "errors"

var (
	ErrAbortedByUser            = errors.New("aborted by user")
	ErrBucketNotEmpty           = errors.New("bucket not empty")
	ErrBucketPolicyBucketNeeded = errors.New("bucket-policy-bucket required")
	ErrBucketPolicyNeeded       = errors.New("bucket-policy required")
	ErrBucketRequired           = errors.New("bucket required")
	ErrDataDirRequired          = errors.New("data dir required")
	ErrKeyAccessBucketNeeded    = errors.New("key-access and key-bucket required")
	ErrKeyAccessNeeded          = errors.New("key-access required")
	ErrKeyAccessSecretNeeded    = errors.New("key-access and key-secret required")
	ErrMetaPathRequired         = errors.New("meta path required")
)
