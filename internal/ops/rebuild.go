package ops

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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
	type rebuildEntry struct {
		path      string
		bucket    string
		key       string
		versionID string
		size      int64
		chunks    []manifest.ChunkRef
		modTime   time.Time
	}
	var entries []rebuildEntry
	if err := store.FlushWith([]func(tx *sql.Tx) error{
		func(tx *sql.Tx) error {
			for _, path := range manifests {
				info, err := os.Stat(path)
				if err != nil {
					return err
				}
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
				bucket, key, ok := parseManifestName(filepath.Base(path))
				if !ok || bucket == "" || key == "" {
					bucket = man.Bucket
					key = man.Key
				}
				if bucket == "" || key == "" {
					report.SkippedManifests++
					continue
				}
				entries = append(entries, rebuildEntry{
					path:      path,
					bucket:    bucket,
					key:       key,
					versionID: man.VersionID,
					size:      man.Size,
					chunks:    man.Chunks,
					modTime:   info.ModTime().UTC(),
				})
			}
			sort.Slice(entries, func(i, j int) bool {
				if entries[i].modTime.Before(entries[j].modTime) {
					return true
				}
				if entries[i].modTime.After(entries[j].modTime) {
					return false
				}
				if entries[i].bucket != entries[j].bucket {
					return entries[i].bucket < entries[j].bucket
				}
				if entries[i].key != entries[j].key {
					return entries[i].key < entries[j].key
				}
				if entries[i].versionID != entries[j].versionID {
					return entries[i].versionID < entries[j].versionID
				}
				return entries[i].path < entries[j].path
			})
			lastPhysical := int64(-1)
			var logical uint32
			for _, entry := range entries {
				physical := entry.modTime.UnixNano()
				if physical == lastPhysical {
					logical++
				} else {
					lastPhysical = physical
					logical = 0
				}
				hlcTS := fmt.Sprintf("%019d-%010d", physical, logical)
				lastModified := entry.modTime.Format(time.RFC3339Nano)
				if err := store.RecordPutWithHLC(tx, hlcTS, "local", entry.bucket, entry.key, entry.versionID, "", entry.size, entry.path, lastModified, true); err != nil {
					return err
				}
				report.RebuiltObjects++
				for _, ch := range entry.chunks {
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

func parseManifestName(name string) (bucket, key string, ok bool) {
	parts := splitN(name, "__", 3)
	if len(parts) < 3 {
		return "", "", false
	}
	return parts[0], parts[1], true
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
