package ops

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
	"github.com/kk-code-lab/seglake/internal/storage/segment"
)

// RebuildIndex rebuilds sqlite metadata using manifests.
func RebuildIndex(layout fs.Layout, metaPath string) (*Report, error) {
	if metaPath == "" {
		return nil, errors.New("ops: meta path required")
	}
	report := newReport("rebuild-index")
	manifests, err := listFiles(layout.ManifestsDir)
	if err != nil {
		return nil, err
	}
	report.Manifests = len(manifests)

	newPath := metaPath + ".new"
	_ = os.Remove(newPath)
	store, err := meta.Open(newPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()

	segmentRefs := make(map[string]struct{})
	if err := store.FlushWith([]func(tx *sql.Tx) error{
		func(tx *sql.Tx) error {
			for _, path := range manifests {
				file, err := os.Open(path)
				if err != nil {
					return err
				}
				man, err := (&manifest.BinaryCodec{}).Decode(file)
				_ = file.Close()
				if err != nil {
					return err
				}
				// Derive bucket/key from manifest filename: expects "<bucket>__<key>__<version>".
				bucket, key := parseManifestName(filepath.Base(path))
				if bucket == "" || key == "" {
					bucket = man.Bucket
					key = man.Key
				}
				if bucket == "" || key == "" {
					report.SkippedManifests++
					continue
				}
				if err := store.RecordPutTx(tx, bucket, key, man.VersionID, "", man.Size, path); err != nil {
					return err
				}
				report.RebuiltObjects++
				for _, ch := range man.Chunks {
					segmentRefs[ch.SegmentID] = struct{}{}
				}
			}
			for segID := range segmentRefs {
				segPath := layout.SegmentPath(segID)
				info, err := os.Stat(segPath)
				if err != nil {
					report.MissingSegments++
					continue
				}
				state := string(segment.StateOpen)
				var footerChecksum []byte
				reader, err := segment.NewReader(segPath)
				if err == nil {
					footer, err := reader.ReadFooter()
					if err == nil {
						state = string(segment.StateSealed)
						footerChecksum = footer.ChecksumHash[:]
					}
					_ = reader.Close()
				}
				if err := store.RecordSegmentTx(tx, segID, segPath, state, info.Size(), footerChecksum); err != nil {
					return err
				}
			}
			return nil
		},
	}); err != nil {
		return nil, err
	}
	// Replace old meta
	_ = os.Remove(metaPath + ".bak")
	_ = os.Rename(metaPath, metaPath+".bak")
	if err := os.Rename(newPath, metaPath); err != nil {
		return nil, err
	}
	report.FinishedAt = time.Now().UTC()
	return report, nil
}

func parseManifestName(name string) (bucket, key string) {
	parts := splitN(name, "__", 3)
	if len(parts) < 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

func splitN(s, sep string, n int) []string {
	if n <= 0 {
		return nil
	}
	var out []string
	for i := 0; i < n-1; i++ {
		idx := strings.Index(s, sep)
		if idx < 0 {
			break
		}
		out = append(out, s[:idx])
		s = s[idx+len(sep):]
	}
	out = append(out, s)
	return out
}
