package s3

import (
	"io"
	"strings"
	"testing"
)

func TestParsePayloadHash(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		header     string
		wantHash   string
		wantVerify bool
		wantErr    bool
	}{
		{
			name:       "empty",
			header:     "",
			wantHash:   "",
			wantVerify: false,
			wantErr:    false,
		},
		{
			name:       "unsigned",
			header:     "UNSIGNED-PAYLOAD",
			wantHash:   "UNSIGNED-PAYLOAD",
			wantVerify: false,
			wantErr:    false,
		},
		{
			name:       "streaming unsigned",
			header:     "STREAMING-UNSIGNED-PAYLOAD",
			wantHash:   "UNSIGNED-PAYLOAD",
			wantVerify: false,
			wantErr:    false,
		},
		{
			name:       "streaming unsigned trailer",
			header:     "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
			wantHash:   "UNSIGNED-PAYLOAD",
			wantVerify: false,
			wantErr:    false,
		},
		{
			name:       "valid hash",
			header:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			wantHash:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			wantVerify: true,
			wantErr:    false,
		},
		{
			name:       "invalid streaming",
			header:     "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
			wantHash:   "",
			wantVerify: false,
			wantErr:    true,
		},
		{
			name:       "invalid length",
			header:     "abcd",
			wantHash:   "",
			wantVerify: false,
			wantErr:    true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotHash, gotVerify, err := parsePayloadHash(tc.header)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotHash != tc.wantHash || gotVerify != tc.wantVerify {
				t.Fatalf("got hash=%q verify=%v, want hash=%q verify=%v", gotHash, gotVerify, tc.wantHash, tc.wantVerify)
			}
		})
	}
}

func TestAWSChunkedReader(t *testing.T) {
	t.Parallel()

	body := strings.Join([]string{
		"4;chunk-signature=deadbeef",
		"Wiki",
		"5;chunk-signature=deadbeef",
		"pedia",
		"0;chunk-signature=deadbeef",
		"x-amz-checksum-crc64nvme: 0000",
		"",
	}, "\r\n")

	r := newAWSChunkedReader(strings.NewReader(body))
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if string(got) != "Wikipedia" {
		t.Fatalf("expected decoded payload, got %q", string(got))
	}
}
