package fs

import "path/filepath"

// Layout defines on-disk directory layout for storage data.
type Layout struct {
	Root         string
	SegmentsDir  string
	ManifestsDir string
}

// NewLayout builds a default layout under the given root.
func NewLayout(root string) Layout {
	return Layout{
		Root:         root,
		SegmentsDir:  filepath.Join(root, "segments"),
		ManifestsDir: filepath.Join(root, "manifests"),
	}
}

func (l Layout) SegmentPath(segmentID string) string {
	return filepath.Join(l.SegmentsDir, segmentID)
}

func (l Layout) ManifestPath(versionID string) string {
	return filepath.Join(l.ManifestsDir, versionID)
}
