package main

import (
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/repl"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
)

func runReplBootstrap(remote, accessKey, secretKey, region, dataDir string, force bool) error {
	return repl.RunBootstrap(remote, accessKey, secretKey, region, dataDir, force)
}

func runReplPull(remote, since string, limit int, fetchData, watch bool, interval, backoffMax, retryTimeout time.Duration, accessKey, secretKey, region string, store *meta.Store, eng *engine.Engine) error {
	return repl.RunPull(remote, since, limit, fetchData, watch, interval, backoffMax, retryTimeout, accessKey, secretKey, region, store, eng)
}

func runReplPush(remote, since string, limit int, watch bool, interval, backoffMax time.Duration, accessKey, secretKey, region string, store *meta.Store) error {
	return repl.RunPush(remote, since, limit, watch, interval, backoffMax, accessKey, secretKey, region, store)
}
