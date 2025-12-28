package s3

import "testing"

func FuzzParsePayloadHash(f *testing.F) {
	// Future: add seed corpus with mixed-case hex and invalid lengths.
	f.Add("")
	f.Add("UNSIGNED-PAYLOAD")
	f.Add("STREAMING-UNSIGNED-PAYLOAD")
	f.Add("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	f.Fuzz(func(t *testing.T, header string) {
		_, _, _ = parsePayloadHash(header)
	})
}

func FuzzParseContentMD5(f *testing.F) {
	// Future: add seed corpus with invalid base64 and wrong lengths.
	f.Add("")
	f.Add("1B2M2Y8AsgTpgAmY7PhCfg==")
	f.Fuzz(func(t *testing.T, header string) {
		_, _ = parseContentMD5(header)
	})
}

func FuzzParseInt(f *testing.F) {
	// Future: add seed corpus with very large values and leading zeros.
	f.Add("0")
	f.Add("1")
	f.Add("9223372036854775807")
	f.Fuzz(func(t *testing.T, s string) {
		_, _ = parseInt(s)
	})
}
