package s3

import (
	"errors"
	"net"
	"strings"
)

var errInvalidBucketName = errors.New("invalid bucket name")

// ValidateBucketName enforces common S3 bucket naming rules.
func ValidateBucketName(bucket string) error {
	if len(bucket) < 3 || len(bucket) > 63 {
		return errInvalidBucketName
	}
	if strings.HasPrefix(bucket, "xn--") || strings.HasPrefix(bucket, "sthree-") || strings.HasPrefix(bucket, "amzn-s3-demo-") {
		return errInvalidBucketName
	}
	if strings.HasSuffix(bucket, "-s3alias") || strings.HasSuffix(bucket, "--ol-s3") || strings.HasSuffix(bucket, ".mrap") || strings.HasSuffix(bucket, "--x-s3") || strings.HasSuffix(bucket, "--table-s3") {
		return errInvalidBucketName
	}
	for _, r := range bucket {
		if r >= 'a' && r <= 'z' {
			continue
		}
		if r >= '0' && r <= '9' {
			continue
		}
		if r == '.' || r == '-' {
			continue
		}
		return errInvalidBucketName
	}
	if !isLowerAlphaNum(bucket[0]) || !isLowerAlphaNum(bucket[len(bucket)-1]) {
		return errInvalidBucketName
	}
	if strings.Contains(bucket, "..") {
		return errInvalidBucketName
	}
	if strings.Contains(bucket, ".-") || strings.Contains(bucket, "-.") {
		return errInvalidBucketName
	}
	if net.ParseIP(bucket) != nil {
		return errInvalidBucketName
	}
	labels := strings.Split(bucket, ".")
	for _, label := range labels {
		if label == "" {
			return errInvalidBucketName
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return errInvalidBucketName
		}
	}
	return nil
}

func isLowerAlphaNum(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9')
}
