package admin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

type requestIDKey struct{}

func TokenHeader() string {
	return tokenHeader
}

func newRequestID() string {
	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(raw)
}

func withRequestID(ctx context.Context, reqID string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, reqID)
}
