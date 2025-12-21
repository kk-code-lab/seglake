package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/kk-code-lab/seglake/internal/app"
	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/s3"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
)

func main() {
	showVersion := flag.Bool("version", false, "Print version and exit")
	showVersionShort := flag.Bool("v", false, "Print version and exit (shorthand)")
	addr := flag.String("addr", ":9000", "HTTP listen address")
	dataDir := flag.String("data-dir", "./data", "Data directory")
	flag.Parse()

	if *showVersion || *showVersionShort {
		fmt.Printf("seglake %s (commit %s)\n", app.Version, app.BuildCommit)
		return
	}

	if flag.NArg() > 0 {
		fmt.Fprintln(os.Stderr, "unknown arguments:", flag.Args())
		os.Exit(2)
	}

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "data dir error: %v\n", err)
		os.Exit(1)
	}

	metaPath := filepath.Join(*dataDir, "meta.db")
	store, err := meta.Open(metaPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "meta open error: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	eng, err := engine.New(engine.Options{
		Layout:    fs.NewLayout(filepath.Join(*dataDir, "objects")),
		MetaStore: store,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "engine init error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("seglake %s (commit %s)\n", app.Version, app.BuildCommit)
	handler := &s3.Handler{
		Engine: eng,
		Meta:   store,
	}
	if err := http.ListenAndServe(*addr, handler); err != nil {
		fmt.Fprintf(os.Stderr, "listen error: %v\n", err)
		os.Exit(1)
	}
}
