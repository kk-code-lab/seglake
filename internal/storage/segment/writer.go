package segment

import (
	"errors"
	"io"
	"os"
)

// Writer appends chunk records to a segment.
type Writer struct {
	path string
	file *os.File
}

// NewWriter opens a segment file for append. If the file is new, it writes the segment header.
// Caller owns Close.
func NewWriter(path string, version uint32) (*Writer, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if info.Size() == 0 {
		if err := EncodeSegmentHeader(file, NewSegmentHeader(version)); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &Writer{path: path, file: file}, nil
}

// AppendRecord writes a chunk record header + data.
// Returns the offset of the data payload within the segment.
func (w *Writer) AppendRecord(header ChunkRecordHeader, data []byte) (int64, error) {
	if w.file == nil {
		return 0, errors.New("segment: writer closed")
	}
	if err := ValidateHeader(header); err != nil {
		return 0, err
	}
	if int(header.Len) != len(data) {
		return 0, errors.New("segment: header length mismatch")
	}

	pos, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	if err := EncodeHeader(w.file, header); err != nil {
		return 0, err
	}
	if _, err := w.file.Write(data); err != nil {
		return 0, err
	}
	return pos + headerLen, nil
}

// Seal writes the footer at the end of the segment.
func (w *Writer) Seal(footer Footer) error {
	if w.file == nil {
		return errors.New("segment: writer closed")
	}
	footer = FinalizeFooter(footer)
	if err := ValidateFooter(footer); err != nil {
		return err
	}
	_, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	return EncodeFooter(w.file, footer)
}

// SealWithIndex writes optional bloom/index data and the footer.
func (w *Writer) SealWithIndex(footer Footer, bloom, index []byte) (Footer, error) {
	if w.file == nil {
		return Footer{}, errors.New("segment: writer closed")
	}
	pos, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return Footer{}, err
	}
	if len(bloom) > 0 {
		footer.BloomOffset = pos
		footer.BloomBytes = uint32(len(bloom))
		if _, err := w.file.Write(bloom); err != nil {
			return Footer{}, err
		}
		pos += int64(len(bloom))
	} else {
		footer.BloomOffset = 0
		footer.BloomBytes = 0
	}
	if len(index) > 0 {
		footer.IndexOffset = pos
		footer.IndexBytes = uint32(len(index))
		if _, err := w.file.Write(index); err != nil {
			return Footer{}, err
		}
	} else {
		footer.IndexOffset = 0
		footer.IndexBytes = 0
	}
	footer = FinalizeFooter(footer)
	if err := ValidateFooter(footer); err != nil {
		return Footer{}, err
	}
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return Footer{}, err
	}
	if err := EncodeFooter(w.file, footer); err != nil {
		return Footer{}, err
	}
	return footer, nil
}

// Close closes the underlying file.
func (w *Writer) Close() error {
	if w.file == nil {
		return nil
	}
	return w.file.Close()
}

// Sync flushes file contents to disk.
func (w *Writer) Sync() error {
	if w.file == nil {
		return nil
	}
	return w.file.Sync()
}
