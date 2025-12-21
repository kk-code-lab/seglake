package main

import (
	"fmt"

	"github.com/kk-code-lab/seglake/internal/app"
)

func main() {
	fmt.Printf("seglake (commit %s)\n", app.BuildCommit)
}
