package segment

import (
	"bytes"
	"testing"
)

func FuzzSegmentDecoders(f *testing.F) {
	// Future: build structured inputs with valid magic/version to reach deeper validation paths.
	f.Add([]byte("seed"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeSegmentHeader(bytes.NewReader(data))
		_, _ = DecodeHeader(bytes.NewReader(data))
		_, _ = DecodeFooter(bytes.NewReader(data))
	})
}
