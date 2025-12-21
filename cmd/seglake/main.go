package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/kk-code-lab/seglake/internal/app"
)

func main() {
	showVersion := flag.Bool("version", false, "Print version and exit")
	showVersionShort := flag.Bool("v", false, "Print version and exit (shorthand)")
	flag.Parse()

	if *showVersion || *showVersionShort {
		fmt.Printf("seglake %s (commit %s)\n", app.Version, app.BuildCommit)
		return
	}

	if flag.NArg() > 0 {
		fmt.Fprintln(os.Stderr, "unknown arguments:", flag.Args())
		os.Exit(2)
	}

	fmt.Printf("seglake %s (commit %s)\n", app.Version, app.BuildCommit)
}
