package segment

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/zeebo/blake3"
)

// State represents segment lifecycle state.
type State string

const (
	StateOpen   State = "OPEN"
	StateSealed State = "SEALED"
)

// ChunkRecordHeader is the fixed header for a chunk record.
type ChunkRecordHeader struct {
	Hash [32]byte
	Len  uint32
}

const (
	headerLen = 32 + 4
)

// SegmentHeader is written at the start of each segment file.
type SegmentHeader struct {
	Magic   uint32
	Version uint32
}

const (
	segmentMagic  = 0x53474c53 // "SGLS"
	segmentHdrLen = 4 + 4
)

// EncodeSegmentHeader writes a segment header to the writer.
func EncodeSegmentHeader(w io.Writer, header SegmentHeader) error {
	var buf [segmentHdrLen]byte
	binary.LittleEndian.PutUint32(buf[0:4], header.Magic)
	binary.LittleEndian.PutUint32(buf[4:8], header.Version)
	_, err := w.Write(buf[:])
	return err
}

// DecodeSegmentHeader reads a segment header from the reader.
func DecodeSegmentHeader(r io.Reader) (SegmentHeader, error) {
	var header SegmentHeader
	var buf [segmentHdrLen]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return SegmentHeader{}, err
	}
	header.Magic = binary.LittleEndian.Uint32(buf[0:4])
	header.Version = binary.LittleEndian.Uint32(buf[4:8])
	return header, nil
}

// NewSegmentHeader returns a header initialized with the magic and version.
func NewSegmentHeader(version uint32) SegmentHeader {
	return SegmentHeader{
		Magic:   segmentMagic,
		Version: version,
	}
}

// ValidateSegmentHeader checks basic invariants and magic.
func ValidateSegmentHeader(h SegmentHeader) error {
	if h.Magic != segmentMagic {
		return fmt.Errorf("segment: invalid header magic")
	}
	return nil
}

// EncodeHeader writes a binary chunk header to the writer.
func EncodeHeader(w io.Writer, header ChunkRecordHeader) error {
	if _, err := w.Write(header.Hash[:]); err != nil {
		return err
	}
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], header.Len)
	_, err := w.Write(buf[:])
	return err
}

// DecodeHeader reads a binary chunk header from the reader.
func DecodeHeader(r io.Reader) (ChunkRecordHeader, error) {
	var header ChunkRecordHeader
	if _, err := io.ReadFull(r, header.Hash[:]); err != nil {
		return ChunkRecordHeader{}, err
	}
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return ChunkRecordHeader{}, err
	}
	header.Len = binary.LittleEndian.Uint32(buf[:])
	return header, nil
}

// ValidateHeader checks basic invariants.
func ValidateHeader(h ChunkRecordHeader) error {
	if h.Len == 0 {
		return fmt.Errorf("segment: zero-length chunk")
	}
	return nil
}

// Footer is written when a segment is sealed.
type Footer struct {
	Magic        uint32
	Version      uint32
	BloomOffset  int64
	IndexOffset  int64
	ChecksumHash [32]byte
}

const (
	footerMagic = 0x53474c4b // "SGLK"
	footerLen   = 4 + 4 + 8 + 8 + 32
)

// EncodeFooter writes a binary footer to the writer.
func EncodeFooter(w io.Writer, footer Footer) error {
	var buf [footerLen]byte
	binary.LittleEndian.PutUint32(buf[0:4], footer.Magic)
	binary.LittleEndian.PutUint32(buf[4:8], footer.Version)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(footer.BloomOffset))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(footer.IndexOffset))
	copy(buf[24:], footer.ChecksumHash[:])
	_, err := w.Write(buf[:])
	return err
}

// DecodeFooter reads a binary footer from the reader.
func DecodeFooter(r io.Reader) (Footer, error) {
	var footer Footer
	var buf [footerLen]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return Footer{}, err
	}
	footer.Magic = binary.LittleEndian.Uint32(buf[0:4])
	footer.Version = binary.LittleEndian.Uint32(buf[4:8])
	footer.BloomOffset = int64(binary.LittleEndian.Uint64(buf[8:16]))
	footer.IndexOffset = int64(binary.LittleEndian.Uint64(buf[16:24]))
	copy(footer.ChecksumHash[:], buf[24:])
	return footer, nil
}

// NewFooter returns a footer initialized with the magic and version.
func NewFooter(version uint32) Footer {
	return Footer{
		Magic:   footerMagic,
		Version: version,
	}
}

// ValidateFooter checks basic invariants and magic.
func ValidateFooter(f Footer) error {
	if f.Magic != footerMagic {
		return fmt.Errorf("segment: invalid footer magic")
	}
	if !isZeroHash(f.ChecksumHash) {
		if want := FooterChecksum(f); want != f.ChecksumHash {
			return fmt.Errorf("segment: footer checksum mismatch")
		}
	}
	return nil
}

// FooterChecksum computes the footer checksum with the checksum field zeroed.
func FooterChecksum(f Footer) [32]byte {
	f.ChecksumHash = [32]byte{}
	var buf [footerLen]byte
	binary.LittleEndian.PutUint32(buf[0:4], f.Magic)
	binary.LittleEndian.PutUint32(buf[4:8], f.Version)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(f.BloomOffset))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(f.IndexOffset))
	copy(buf[24:], f.ChecksumHash[:])
	return blake3.Sum256(buf[:])
}

// FinalizeFooter fills the checksum field if it is empty.
func FinalizeFooter(f Footer) Footer {
	if isZeroHash(f.ChecksumHash) {
		f.ChecksumHash = FooterChecksum(f)
	}
	return f
}

func isZeroHash(h [32]byte) bool {
	for _, b := range h {
		if b != 0 {
			return false
		}
	}
	return true
}
