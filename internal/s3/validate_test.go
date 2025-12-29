package s3

import (
	"strings"
	"testing"
)

func TestValidateBucketName(t *testing.T) {
	valid := []string{
		"abc",
		"my-bucket",
		"my.bucket",
		"a1b2c3",
		"demo-123",
		"a.b.c",
	}
	for _, name := range valid {
		if err := ValidateBucketName(name); err != nil {
			t.Fatalf("expected valid bucket %q: %v", name, err)
		}
	}

	invalid := []string{
		"",
		"ab",
		strings.Repeat("a", 64),
		"MyBucket",
		"xn--bucket",
		"sthree-bucket",
		"amzn-s3-demo-bucket",
		"bucket-s3alias",
		"bucket--ol-s3",
		"bucket.mrap",
		"bucket--x-s3",
		"bucket--table-s3",
		"-abc",
		"abc-",
		"a..b",
		"a_b",
		"192.168.1.1",
		".abc",
		"abc.",
		"a.-b",
		"a-.b",
	}
	for _, name := range invalid {
		if err := ValidateBucketName(name); err == nil {
			t.Fatalf("expected invalid bucket %q", name)
		}
	}
}
