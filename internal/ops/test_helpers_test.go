package ops

import (
	"os"
	"path/filepath"

	"github.com/kk-code-lab/seglake/internal/storage/manifest"
)

func writeManifest(path string, man *manifest.Manifest) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return (&manifest.BinaryCodec{}).Encode(file, man)
}
