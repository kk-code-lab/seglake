package ops

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestCrashHarness(t *testing.T) {
	if os.Getenv("SEGLAKE_CRASH_TEST") == "" {
		t.Skip("set SEGLAKE_CRASH_TEST=1 to run crash harness")
	}

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	root := filepath.Clean(filepath.Join(wd, "..", ".."))
	dataDir := t.TempDir()

	cmd := exec.Command("bash", "scripts/crash_harness.sh", "1")
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"DATA_DIR="+dataDir,
		"ADDR=:19100",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("crash_harness failed: %v\n%s", err, string(out))
	}
}
