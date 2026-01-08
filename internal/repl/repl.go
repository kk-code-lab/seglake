package repl

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/s3"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
	storagefs "github.com/kk-code-lab/seglake/internal/storage/fs"
)

type replClient struct {
	base   *url.URL
	client *http.Client
	signer *s3.AuthConfig
}

type replOplogResponse struct {
	Entries []meta.OplogEntry `json:"entries"`
	LastHLC string            `json:"last_hlc,omitempty"`
}

type replOplogApplyRequest struct {
	Entries []meta.OplogEntry `json:"entries"`
}

type replMissingChunk struct {
	SegmentID string `json:"segment_id"`
	Offset    int64  `json:"offset"`
	Length    int64  `json:"len"`
}

type replOplogApplyResponse struct {
	Applied          int                `json:"applied"`
	MissingManifests []string           `json:"missing_manifests,omitempty"`
	MissingChunks    []replMissingChunk `json:"missing_chunks,omitempty"`
}

func RunBootstrap(remote, accessKey, secretKey, region, dataDir string, force bool) error {
	if remote == "" {
		return fmt.Errorf("replication: -repl-remote required")
	}
	base, err := url.Parse(remote)
	if err != nil {
		return err
	}
	if base.Scheme == "" {
		base.Scheme = "http"
	}
	if base.Host == "" && base.Path != "" && !strings.Contains(base.Path, "/") {
		base.Host = base.Path
		base.Path = ""
	}
	client := &replClient{
		base: base,
		client: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
	if accessKey != "" && secretKey != "" {
		if region == "" {
			region = "us-east-1"
		}
		client.signer = &s3.AuthConfig{
			AccessKey:            accessKey,
			SecretKey:            secretKey,
			Region:               region,
			AllowUnsignedPayload: true,
		}
	}
	if dataDir == "" {
		dataDir = "./data"
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	metaPath := filepath.Join(dataDir, "meta.db")
	if !force {
		if _, err := os.Stat(metaPath); err == nil {
			return fmt.Errorf("replication: meta.db exists (use -repl-bootstrap-force to overwrite)")
		}
	}
	tmpDir, err := os.MkdirTemp("", "seglake-bootstrap-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	resp, err := client.getSnapshot()
	if err != nil {
		return err
	}
	if err := extractTarGz(resp, tmpDir); err != nil {
		return err
	}
	if err := resp.Close(); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(tmpDir, "meta.db")); err != nil {
		return fmt.Errorf("replication: snapshot missing meta.db: %w", err)
	}
	if err := replaceMetaFiles(tmpDir, dataDir, force); err != nil {
		return err
	}

	store, err := meta.Open(metaPath)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	layout := storagefs.NewLayout(filepath.Join(dataDir, "objects"))
	eng, err := engine.New(engine.Options{Layout: layout, MetaStore: store})
	if err != nil {
		return err
	}
	since, err := store.MaxOplogHLC(context.Background())
	if err != nil {
		return err
	}
	if err := RunPull(remote, since, 1000, true, false, 0, 0, 5*time.Minute, accessKey, secretKey, region, store, eng); err != nil {
		return err
	}
	return nil
}

type replMissingCache struct {
	chunks map[string]replMissingChunk
}

func newReplMissingCache() *replMissingCache {
	return &replMissingCache{chunks: make(map[string]replMissingChunk)}
}

func (c *replMissingCache) addChunk(ch replMissingChunk) {
	if c == nil {
		return
	}
	if c.chunks == nil {
		c.chunks = make(map[string]replMissingChunk)
	}
	c.chunks[chunkKey(ch)] = ch
}

func (c *replMissingCache) addChunks(chunks []replMissingChunk) {
	if c == nil {
		return
	}
	for _, ch := range chunks {
		c.addChunk(ch)
	}
}

func (c *replMissingCache) snapshot() map[string]replMissingChunk {
	if c == nil {
		return nil
	}
	out := make(map[string]replMissingChunk, len(c.chunks))
	for k, v := range c.chunks {
		out[k] = v
	}
	return out
}

func (c *replMissingCache) clear() {
	if c == nil {
		return
	}
	c.chunks = make(map[string]replMissingChunk)
}

func RunPush(remote, since string, limit int, watch bool, interval, backoffMax time.Duration, accessKey, secretKey, region string, store *meta.Store) error {
	if store == nil {
		return fmt.Errorf("replication: store required")
	}
	if remote == "" {
		return fmt.Errorf("replication: -repl-remote required")
	}
	base, err := url.Parse(remote)
	if err != nil {
		return err
	}
	if base.Scheme == "" {
		base.Scheme = "http"
	}
	if base.Host == "" && base.Path != "" && !strings.Contains(base.Path, "/") {
		base.Host = base.Path
		base.Path = ""
	}
	client := &replClient{
		base: base,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
	remoteKey := replRemoteKey(base)
	if accessKey != "" && secretKey != "" {
		if region == "" {
			region = "us-east-1"
		}
		client.signer = &s3.AuthConfig{
			AccessKey:            accessKey,
			SecretKey:            secretKey,
			Region:               region,
			AllowUnsignedPayload: true,
		}
	}
	if limit <= 0 {
		limit = 1000
	}
	ctx := context.Background()
	if since == "" {
		if hlc, err := store.GetReplRemotePushWatermark(ctx, remoteKey); err == nil && hlc != "" {
			since = hlc
		}
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if backoffMax <= 0 {
		backoffMax = time.Minute
	}
	backoff := interval
	for {
		lastHLC, pushed, applied, err := runReplPushOnce(ctx, client, store, since, limit)
		if err != nil {
			if !watch {
				return err
			}
			fmt.Printf("repl: error=%v backoff=%s\n", err, backoff)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > backoffMax {
				backoff = backoffMax
			}
			continue
		}
		backoff = interval
		if lastHLC != "" {
			since = lastHLC
		}
		if !watch {
			return nil
		}
		if pushed == 0 || applied == 0 {
			time.Sleep(interval)
		}
	}
}

func RunPull(remote, since string, limit int, fetchData bool, watch bool, interval, backoffMax, retryTimeout time.Duration, accessKey, secretKey, region string, store *meta.Store, eng *engine.Engine) error {
	if eng == nil {
		return fmt.Errorf("replication: engine required")
	}
	if remote == "" {
		return fmt.Errorf("replication: -repl-remote required")
	}
	base, err := url.Parse(remote)
	if err != nil {
		return err
	}
	if base.Scheme == "" {
		base.Scheme = "http"
	}
	if base.Host == "" && base.Path != "" && !strings.Contains(base.Path, "/") {
		base.Host = base.Path
		base.Path = ""
	}
	client := &replClient{
		base: base,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
	remoteKey := replRemoteKey(base)
	if accessKey != "" && secretKey != "" {
		if region == "" {
			region = "us-east-1"
		}
		client.signer = &s3.AuthConfig{
			AccessKey:            accessKey,
			SecretKey:            secretKey,
			Region:               region,
			AllowUnsignedPayload: true,
		}
	}
	if limit <= 0 {
		limit = 1000
	}
	ctx := context.Background()
	if since == "" && store != nil {
		if hlc, err := store.GetReplRemotePullWatermark(ctx, remoteKey); err == nil && hlc != "" {
			since = hlc
		}
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if backoffMax <= 0 {
		backoffMax = time.Minute
	}
	backoff := interval
	missingCache := newReplMissingCache()
	if retryTimeout <= 0 {
		retryTimeout = 5 * time.Minute
	}
	retryDeadline := now().Add(retryTimeout)
	for {
		lastHLC, applied, err := runReplPullOnce(ctx, client, since, limit, fetchData, store, eng, missingCache, retryDeadline)
		if err != nil {
			if !watch {
				return err
			}
			fmt.Printf("repl: error=%v backoff=%s\n", err, backoff)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > backoffMax {
				backoff = backoffMax
			}
			continue
		}
		backoff = interval
		if lastHLC != "" {
			since = lastHLC
			if store != nil {
				_ = store.SetReplRemotePullWatermark(ctx, remoteKey, lastHLC)
			}
		}
		if !watch {
			return nil
		}
		if applied == 0 {
			time.Sleep(interval)
		}
		if watch {
			retryDeadline = now().Add(retryTimeout)
		}
	}
}

func chunkKey(ch replMissingChunk) string {
	return fmt.Sprintf("%s:%d:%d", ch.SegmentID, ch.Offset, ch.Length)
}

func fetchChunkWithRetry(ctx context.Context, client *replClient, eng *engine.Engine, ch replMissingChunk, attempts int, deadline time.Time) error {
	if attempts <= 0 {
		attempts = 1
	}
	backoff := 200 * time.Millisecond
	var lastErr error
	for i := 0; i < attempts; i++ {
		if !deadline.IsZero() && now().After(deadline) {
			return fmt.Errorf("repl: retry deadline exceeded")
		}
		data, err := client.getChunk(ch.SegmentID, ch.Offset, ch.Length)
		if err == nil {
			if err := eng.WriteSegmentRange(ctx, ch.SegmentID, ch.Offset, data); err != nil {
				return err
			}
			return nil
		}
		lastErr = err
		time.Sleep(backoff)
		backoff *= 2
	}
	return lastErr
}

func runReplPullOnce(ctx context.Context, client *replClient, since string, limit int, fetchData bool, store *meta.Store, eng *engine.Engine, cache *replMissingCache, retryDeadline time.Time) (string, int, error) {
	oplogResp, err := client.getOplog(since, limit)
	if err != nil {
		return "", 0, err
	}
	if len(oplogResp.Entries) == 0 {
		fmt.Println("repl: no new oplog entries")
		return oplogResp.LastHLC, 0, nil
	}
	if store == nil {
		return "", 0, errors.New("repl: store required")
	}
	applied, err := store.ApplyOplogEntries(ctx, oplogResp.Entries)
	if err != nil {
		return "", 0, err
	}
	fmt.Printf("repl: applied=%d remote_last_hlc=%s\n", applied, oplogResp.LastHLC)

	if !fetchData {
		return oplogResp.LastHLC, applied, nil
	}
	missingManifests := make(map[string]struct{})
	missingChunks := make(map[string]replMissingChunk)
	if eng != nil {
		for _, entry := range oplogResp.Entries {
			if entry.OpType != "put" || entry.VersionID == "" {
				continue
			}
			man, err := eng.GetManifest(ctx, entry.VersionID)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					missingManifests[entry.VersionID] = struct{}{}
					continue
				}
				return "", applied, err
			}
			chunks, err := eng.MissingChunks(man)
			if err != nil {
				return "", applied, err
			}
			for _, ch := range chunks {
				key := chunkKey(replMissingChunk{
					SegmentID: ch.SegmentID,
					Offset:    ch.Offset,
					Length:    ch.Length,
				})
				missingChunks[key] = replMissingChunk{
					SegmentID: ch.SegmentID,
					Offset:    ch.Offset,
					Length:    ch.Length,
				}
			}
		}
	}
	if cache != nil {
		cache.addChunks(mapToChunks(missingChunks))
		for k, v := range cache.snapshot() {
			if _, ok := missingChunks[k]; !ok {
				missingChunks[k] = v
			}
		}
	}
	fetchedManifests := 0
	var fetchedBytes int64
	for versionID := range missingManifests {
		manifestBytes, err := client.getManifest(versionID)
		if err != nil {
			return "", applied, err
		}
		fetchedBytes += int64(len(manifestBytes))
		man, err := eng.StoreManifestBytes(ctx, manifestBytes)
		if err != nil {
			return "", applied, err
		}
		chunks, err := eng.MissingChunks(man)
		if err != nil {
			return "", applied, err
		}
		for _, ch := range chunks {
			key := fmt.Sprintf("%s:%d:%d", ch.SegmentID, ch.Offset, ch.Length)
			if _, ok := missingChunks[key]; !ok {
				missingChunks[key] = replMissingChunk{
					SegmentID: ch.SegmentID,
					Offset:    ch.Offset,
					Length:    ch.Length,
				}
			}
		}
		fetchedManifests++
	}
	fetched := 0
	if len(missingChunks) > 0 {
		for _, ch := range missingChunks {
			if now().After(retryDeadline) {
				return "", applied, errors.New("repl: retry deadline exceeded")
			}
			if err := fetchChunkWithRetry(ctx, client, eng, ch, 3, retryDeadline); err != nil {
				return "", applied, err
			}
			fetchedBytes += ch.Length
			fetched++
		}
	}
	if cache != nil && fetched == len(missingChunks) {
		cache.clear()
	}
	if fetchedManifests > 0 || fetched > 0 {
		fmt.Printf("repl: fetched manifests=%d chunks=%d\n", fetchedManifests, fetched)
	}
	if store != nil && fetchedBytes > 0 {
		if err := store.RecordReplBytes(ctx, fetchedBytes); err != nil {
			return "", applied, err
		}
	}
	return oplogResp.LastHLC, applied, nil
}

func mapToChunks(items map[string]replMissingChunk) []replMissingChunk {
	if len(items) == 0 {
		return nil
	}
	out := make([]replMissingChunk, 0, len(items))
	for _, ch := range items {
		out = append(out, ch)
	}
	return out
}

func runReplPushOnce(ctx context.Context, client *replClient, store *meta.Store, since string, limit int) (string, int, int, error) {
	entries, err := store.ListOplogSince(ctx, since, limit)
	if err != nil {
		return "", 0, 0, err
	}
	if len(entries) == 0 {
		fmt.Println("repl: no local oplog entries to push")
		return since, 0, 0, nil
	}
	resp, err := client.applyOplog(entries)
	if err != nil {
		return "", 0, 0, err
	}
	lastHLC := entries[len(entries)-1].HLCTS
	_ = store.SetReplRemotePushWatermark(ctx, replRemoteKey(client.base), lastHLC)
	fmt.Printf("repl: pushed=%d applied=%d last_hlc=%s\n", len(entries), resp.Applied, lastHLC)
	return lastHLC, len(entries), resp.Applied, nil
}

func replRemoteKey(base *url.URL) string {
	if base == nil {
		return ""
	}
	host := base.Host
	if host == "" && base.Path != "" && !strings.Contains(base.Path, "/") {
		host = base.Path
	}
	if host == "" {
		return base.String()
	}
	scheme := base.Scheme
	if scheme == "" {
		scheme = "http"
	}
	return scheme + "://" + host
}

func (c *replClient) getOplog(since string, limit int) (*replOplogResponse, error) {
	query := url.Values{}
	if since != "" {
		query.Set("since", since)
	}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	resp, err := c.do(http.MethodGet, "/v1/replication/oplog", query, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("oplog fetch failed: status=%d body=%s", resp.StatusCode, string(body))
	}
	var out replOplogResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *replClient) getSnapshot() (io.ReadCloser, error) {
	resp, err := c.do(http.MethodGet, "/v1/replication/snapshot", nil, nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, fmt.Errorf("snapshot fetch failed: status=%d body=%s", resp.StatusCode, string(payload))
	}
	return resp.Body, nil
}

func (c *replClient) applyOplog(entries []meta.OplogEntry) (*replOplogApplyResponse, error) {
	body, err := json.Marshal(replOplogApplyRequest{Entries: entries})
	if err != nil {
		return nil, err
	}
	resp, err := c.do(http.MethodPost, "/v1/replication/oplog", nil, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("oplog apply failed: status=%d body=%s", resp.StatusCode, string(payload))
	}
	var out replOplogApplyResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *replClient) getManifest(versionID string) ([]byte, error) {
	query := url.Values{}
	query.Set("versionId", versionID)
	resp, err := c.do(http.MethodGet, "/v1/replication/manifest", query, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("manifest fetch failed: status=%d body=%s", resp.StatusCode, string(payload))
	}
	return io.ReadAll(resp.Body)
}

func (c *replClient) getChunk(segmentID string, offset, length int64) ([]byte, error) {
	query := url.Values{}
	query.Set("segmentId", segmentID)
	query.Set("offset", strconv.FormatInt(offset, 10))
	query.Set("len", strconv.FormatInt(length, 10))
	resp, err := c.do(http.MethodGet, "/v1/replication/chunk", query, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("chunk fetch failed: status=%d body=%s", resp.StatusCode, string(payload))
	}
	return io.ReadAll(resp.Body)
}

func (c *replClient) do(method, route string, query url.Values, body io.Reader) (*http.Response, error) {
	if c == nil || c.base == nil {
		return nil, errors.New("replication: client not configured")
	}
	rel := &url.URL{Path: path.Clean("/" + strings.TrimPrefix(route, "/"))}
	if len(query) > 0 {
		rel.RawQuery = query.Encode()
	}
	target := c.base.ResolveReference(rel)
	urlStr := target.String()
	if c.signer != nil {
		presigned, err := c.signer.Presign(method, urlStr, 5*time.Minute)
		if err != nil {
			return nil, err
		}
		urlStr = presigned
	}
	req, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		return nil, err
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.client.Do(req)
}

func extractTarGz(r io.Reader, dst string) error {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if hdr.Name == "" {
			continue
		}
		clean := filepath.Clean(hdr.Name)
		if strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) {
			return fmt.Errorf("replication: invalid snapshot path %q", hdr.Name)
		}
		target := filepath.Join(dst, clean)
		if hdr.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		out, err := os.Create(target)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, tr); err != nil {
			_ = out.Close()
			return err
		}
		if err := out.Close(); err != nil {
			return err
		}
	}
	return nil
}

func replaceMetaFiles(srcDir, dataDir string, force bool) error {
	targets := []string{"meta.db", "meta.db-wal", "meta.db-shm"}
	for _, name := range targets {
		src := filepath.Join(srcDir, name)
		if _, err := os.Stat(src); err != nil {
			continue
		}
		dst := filepath.Join(dataDir, name)
		if !force {
			if _, err := os.Stat(dst); err == nil {
				return fmt.Errorf("replication: %s exists (use -repl-bootstrap-force)", name)
			}
		}
		_ = os.Remove(dst)
		if err := copyFile(src, dst); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}
