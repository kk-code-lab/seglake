package ops

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

// ReplValidate compares manifests and live versions between two data directories.
func ReplValidate(layout fs.Layout, metaPath, compareDir string) (*Report, error) {
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

	report.FinishedAt = time.Now().UTC()
	return report, nil
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
