package segment

import (
	"errors"
	"io"
	"os"
)

// Reader reads records from a segment file.
type Reader struct {
	path    string
	file    *os.File
	header  SegmentHeader
	dataEnd int64
	offset  int64
}

// NewReader opens a segment file for reading.
func NewReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	header, err := DecodeSegmentHeader(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := ValidateSegmentHeader(header); err != nil {
		_ = file.Close()
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	dataEnd := info.Size() - footerLen
	if dataEnd < segmentHdrLen {
		_ = file.Close()
		return nil, io.ErrUnexpectedEOF
	}
	if _, err := file.Seek(segmentHdrLen, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &Reader{path: path, file: file, header: header, dataEnd: dataEnd, offset: segmentHdrLen}, nil
}

// ReadRecord reads the next record into the provided buffer.
func (r *Reader) ReadRecord() (ChunkRecordHeader, []byte, error) {
	if r.file == nil {
		return ChunkRecordHeader{}, nil, errors.New("segment: reader closed")
	}
	if r.offset >= r.dataEnd {
		return ChunkRecordHeader{}, nil, io.EOF
	}
	header, err := DecodeHeader(r.file)
	if err != nil {
		return ChunkRecordHeader{}, nil, err
	}
	if err := ValidateHeader(header); err != nil {
		return ChunkRecordHeader{}, nil, err
	}
	data := make([]byte, header.Len)
	if _, err := io.ReadFull(r.file, data); err != nil {
		return ChunkRecordHeader{}, nil, err
	}
	r.offset += headerLen + int64(header.Len)
	return header, data, nil
}

// ReadFooter reads the footer from the end of the segment file.
func (r *Reader) ReadFooter() (Footer, error) {
	if r.file == nil {
		return Footer{}, errors.New("segment: reader closed")
	}
	info, err := r.file.Stat()
	if err != nil {
		return Footer{}, err
	}
	if info.Size() < footerLen {
		return Footer{}, io.ErrUnexpectedEOF
	}
	if _, err := r.file.Seek(-footerLen, io.SeekEnd); err != nil {
		return Footer{}, err
	}
	footer, err := DecodeFooter(r.file)
	if err != nil {
		return Footer{}, err
	}
	if err := ValidateFooter(footer); err != nil {
		return Footer{}, err
	}
	return footer, nil
}

// Header returns the parsed segment header.
func (r *Reader) Header() SegmentHeader {
	return r.header
}

// Close closes the underlying file.
func (r *Reader) Close() error {
	if r.file == nil {
		return nil
	}
	return r.file.Close()
}
