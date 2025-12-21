package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/kk-code-lab/seglake/internal/storage/segment"
)

func (e *Engine) recoverOpenSegments(ctx context.Context) error {
	if e.metaStore == nil {
		return nil
	}
	segs, err := e.metaStore.ListSegments(ctx)
	if err != nil {
		return err
	}
	for _, seg := range segs {
		if seg.State != string(segment.StateOpen) {
			continue
		}
		path := seg.Path
		if path == "" {
			path = e.layout.SegmentPath(seg.ID)
		}
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		reader, err := segment.NewReader(path)
		if err == nil {
			footer, err := reader.ReadFooter()
			_ = reader.Close()
			if err == nil {
				_ = e.metaStore.RecordSegment(ctx, seg.ID, path, string(segment.StateSealed), info.Size(), footer.ChecksumHash[:])
				continue
			}
		}
		version, ok, err := scanOpenSegment(path)
		if err != nil || !ok {
			fmt.Fprintf(os.Stderr, "recover: open segment %s not sealed: %v\n", seg.ID, err)
			continue
		}
		footer := segment.FinalizeFooter(segment.NewFooter(version))
		file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "recover: open segment %s open error: %v\n", seg.ID, err)
			continue
		}
		if err := segment.EncodeFooter(file, footer); err != nil {
			_ = file.Close()
			fmt.Fprintf(os.Stderr, "recover: open segment %s seal error: %v\n", seg.ID, err)
			continue
		}
		_ = file.Sync()
		_ = file.Close()
		info, err = os.Stat(path)
		if err == nil {
			_ = e.metaStore.RecordSegment(ctx, seg.ID, path, string(segment.StateSealed), info.Size(), footer.ChecksumHash[:])
		}
	}
	return nil
}

func scanOpenSegment(path string) (uint32, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = file.Close() }()

	header, err := segment.DecodeSegmentHeader(file)
	if err != nil {
		return 0, false, err
	}
	if err := segment.ValidateSegmentHeader(header); err != nil {
		return 0, false, err
	}
	for {
		record, err := segment.DecodeHeader(file)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return header.Version, true, nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return header.Version, false, err
			}
			return header.Version, false, err
		}
		if err := segment.ValidateHeader(record); err != nil {
			return header.Version, false, err
		}
		if _, err := io.CopyN(io.Discard, file, int64(record.Len)); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return header.Version, false, err
			}
			return header.Version, false, err
		}
	}
}
