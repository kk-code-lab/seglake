package ops

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
	"github.com/kk-code-lab/seglake/internal/storage/segment"
)

var (
	errReplMissingSegment = errors.New("missing segment")
	errReplOutOfBounds    = errors.New("chunk out of bounds")
	errReplHashMismatch   = errors.New("hash mismatch")
)

// ReplValidate compares manifests and live versions between two data directories.
func ReplValidate(layout fs.Layout, metaPath, compareDir string) (*Report, error) {
	return ReplValidateWithOptions(layout, metaPath, compareDir, ReplValidateOptions{})
}

// ReplValidateWithOptions compares replication state and optionally verifies chunk bytes.
func ReplValidateWithOptions(layout fs.Layout, metaPath, compareDir string, opts ReplValidateOptions) (*Report, error) {
	if compareDir == "" {
		return nil, errors.New("ops: repl-validate requires compare dir")
	}
	report := newReport("repl-validate")
	otherLayout := fs.NewLayout(filepath.Join(compareDir, "objects"))
	otherMetaPath := filepath.Join(compareDir, "meta.db")

	localManifests, err := listFiles(layout.ManifestsDir)
	if err != nil {
		return nil, err
	}
	remoteManifests, err := listFiles(otherLayout.ManifestsDir)
	if err != nil {
		return nil, err
	}
	report.CompareManifestsLocal = len(localManifests)
	report.CompareManifestsRemote = len(remoteManifests)

	addError := func(msg string) {
		report.Errors++
		if len(report.ErrorSample) < 5 {
			report.ErrorSample = append(report.ErrorSample, msg)
		}
	}

	localSet := normalizePaths(layout.ManifestsDir, localManifests)
	remoteSet := normalizePaths(otherLayout.ManifestsDir, remoteManifests)
	extraLocal, missingLocal := diffSets(localSet, remoteSet)
	report.CompareManifestsExtra = len(extraLocal)
	report.CompareManifestsMissing = len(missingLocal)
	for _, rel := range extraLocal {
		addError(fmt.Sprintf("manifest missing on remote: %s", rel))
	}
	for _, rel := range missingLocal {
		addError(fmt.Sprintf("manifest missing locally: %s", rel))
	}

	localStore, err := meta.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = localStore.Close() }()
	remoteStore, err := meta.Open(otherMetaPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = remoteStore.Close() }()

	localLive, err := localStore.ListLiveManifestPaths(context.Background())
	if err != nil {
		return nil, err
	}
	remoteLive, err := remoteStore.ListLiveManifestPaths(context.Background())
	if err != nil {
		return nil, err
	}
	localLiveSet := normalizePaths(layout.ManifestsDir, localLive)
	remoteLiveSet := normalizePaths(otherLayout.ManifestsDir, remoteLive)
	report.CompareLiveLocal = len(localLiveSet)
	report.CompareLiveRemote = len(remoteLiveSet)
	extraLive, missingLive := diffSets(localLiveSet, remoteLiveSet)
	report.CompareLiveExtra = len(extraLive)
	report.CompareLiveMissing = len(missingLive)
	for _, rel := range extraLive {
		addError(fmt.Sprintf("live version missing on remote: %s", rel))
	}
	for _, rel := range missingLive {
		addError(fmt.Sprintf("live version missing locally: %s", rel))
	}

	localVersions, err := localStore.ListVersionManifestPaths(context.Background())
	if err != nil {
		return nil, err
	}
	remoteVersions, err := remoteStore.ListVersionManifestPaths(context.Background())
	if err != nil {
		return nil, err
	}
	localVersionSet := normalizePaths(layout.ManifestsDir, localVersions)
	remoteVersionSet := normalizePaths(otherLayout.ManifestsDir, remoteVersions)
	report.CompareVersionsLocal = len(localVersionSet)
	report.CompareVersionsRemote = len(remoteVersionSet)
	extraVersions, missingVersions := diffSets(localVersionSet, remoteVersionSet)
	report.CompareVersionsExtra = len(extraVersions)
	report.CompareVersionsMissing = len(missingVersions)
	for _, rel := range extraVersions {
		addError(fmt.Sprintf("version missing on remote: %s", rel))
	}
	for _, rel := range missingVersions {
		addError(fmt.Sprintf("version missing locally: %s", rel))
	}
	if opts.Deep {
		validateReplChunkHashes(layout, "local", localManifests, report, addError)
		validateReplChunkHashes(otherLayout, "remote", remoteManifests, report, addError)
	}

	report.FinishedAt = now().UTC()
	return report, nil
}

func validateReplChunkHashes(layout fs.Layout, label string, manifestPaths []string, report *Report, addError func(string)) {
	codec := &manifest.BinaryCodec{}
	for _, path := range manifestPaths {
		file, err := os.Open(path)
		if err != nil {
			report.InvalidManifests++
			addError(fmt.Sprintf("%s manifest open failed: %s: %v", label, manifestRel(layout, path), err))
			continue
		}
		man, err := codec.Decode(file)
		_ = file.Close()
		if err != nil {
			report.InvalidManifests++
			addError(fmt.Sprintf("%s manifest invalid: %s: %v", label, manifestRel(layout, path), err))
			continue
		}
		for _, ch := range man.Chunks {
			report.CompareChunksChecked++
			if err := validateReplChunk(layout, ch); err != nil {
				report.CompareChunksInvalid++
				switch {
				case errors.Is(err, errReplMissingSegment):
					report.MissingSegments++
					if len(report.MissingSegmentIDs) < 20 {
						report.MissingSegmentIDs = append(report.MissingSegmentIDs, ch.SegmentID)
					}
				case errors.Is(err, errReplOutOfBounds):
					report.OutOfBoundsChunks++
				}
				addError(fmt.Sprintf("%s chunk invalid manifest=%s version=%s segment=%s offset=%d len=%d: %v", label, manifestRel(layout, path), man.VersionID, ch.SegmentID, ch.Offset, ch.Len, err))
			}
		}
	}
}

func validateReplChunk(layout fs.Layout, ch manifest.ChunkRef) error {
	segPath := layout.SegmentPath(ch.SegmentID)
	info, err := os.Stat(segPath)
	if err != nil {
		if os.IsNotExist(err) {
			return errReplMissingSegment
		}
		return err
	}
	if info.Size() < ch.Offset+int64(ch.Len) {
		return errReplOutOfBounds
	}
	file, err := os.Open(segPath)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	buf := make([]byte, ch.Len)
	n, err := file.ReadAt(buf, ch.Offset)
	if err != nil && err != io.EOF {
		return err
	}
	if n != int(ch.Len) {
		return io.ErrUnexpectedEOF
	}
	if got := segment.HashChunk(buf); got != ch.Hash {
		return errReplHashMismatch
	}
	return nil
}

func manifestRel(layout fs.Layout, path string) string {
	rel, err := filepath.Rel(layout.ManifestsDir, path)
	if err != nil || strings.HasPrefix(rel, "..") {
		return filepath.Base(path)
	}
	return filepath.Clean(rel)
}

func normalizePaths(base string, paths []string) map[string]struct{} {
	out := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		rel, err := filepath.Rel(base, path)
		if err != nil || strings.HasPrefix(rel, "..") {
			rel = filepath.Base(path)
		}
		out[filepath.Clean(rel)] = struct{}{}
	}
	return out
}

func diffSets(a, b map[string]struct{}) (extraA []string, extraB []string) {
	for key := range a {
		if _, ok := b[key]; !ok {
			extraA = append(extraA, key)
		}
	}
	for key := range b {
		if _, ok := a[key]; !ok {
			extraB = append(extraB, key)
		}
	}
	return extraA, extraB
}
