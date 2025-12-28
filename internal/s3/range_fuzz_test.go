package s3

import "testing"

func FuzzParseRange(f *testing.F) {
	// Future: add seed corpus with real-world Range headers (multi-range, suffix, open-ended).
	f.Add("bytes=0-0", int64(1))
	f.Add("bytes=1-3", int64(10))
	f.Add("bytes=-5", int64(10))
	f.Add("bytes=5-", int64(10))
	f.Add("bytes=0-1,3-4", int64(10))
	f.Add("bytes=0-0", int64(0))
	f.Add("bytes=0-", int64(0))
	f.Add("bytes=-1", int64(0))
	f.Add("bytes=0-9", int64(5))
	f.Add("bytes=9-0", int64(10))
	f.Add("bytes=1-1", int64(1))
	f.Add("bytes=1-1,2-2", int64(10))
	f.Fuzz(func(t *testing.T, header string, size int64) {
		start, length, ok := parseRange(header, size)
		if ok {
			if size < 0 {
				t.Fatalf("parseRange ok with negative size=%d", size)
			}
			if start < 0 || (size > 0 && length <= 0) {
				t.Fatalf("parseRange invalid range start=%d length=%d", start, length)
			}
			if size == 0 && length != 0 {
				t.Fatalf("parseRange invalid zero-size length=%d", length)
			}
			if size >= 0 && length > 0 && start > size-length {
				t.Fatalf("parseRange out of bounds start=%d length=%d size=%d", start, length, size)
			}
		}

		ranges, ok := parseRanges(header, size)
		if ok {
			if size < 0 {
				t.Fatalf("parseRanges ok with negative size=%d", size)
			}
			for _, r := range ranges {
				if r.start < 0 || (size > 0 && r.length <= 0) {
					t.Fatalf("parseRanges invalid range start=%d length=%d", r.start, r.length)
				}
				if size == 0 && r.length != 0 {
					t.Fatalf("parseRanges invalid zero-size length=%d", r.length)
				}
				if size >= 0 && r.length > 0 && r.start > size-r.length {
					t.Fatalf("parseRanges out of bounds start=%d length=%d size=%d", r.start, r.length, size)
				}
			}
		}
	})
}
