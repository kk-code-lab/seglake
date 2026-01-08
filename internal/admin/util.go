package admin

import (
	"fmt"

	"github.com/kk-code-lab/seglake/internal/s3"
)

func parsePolicy(policy string) error {
	if policy == "" {
		return fmt.Errorf("policy required")
	}
	if _, err := s3.ParsePolicy(policy); err != nil {
		return fmt.Errorf("invalid policy: %w", err)
	}
	return nil
}

func validateBucketName(name string) error {
	return s3.ValidateBucketName(name)
}
