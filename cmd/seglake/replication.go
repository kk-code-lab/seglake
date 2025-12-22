package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/kk-code-lab/seglake/internal/meta"
	"github.com/kk-code-lab/seglake/internal/s3"
	"github.com/kk-code-lab/seglake/internal/storage/engine"
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

func runReplPush(remote, since string, limit int, watch bool, interval, backoffMax time.Duration, accessKey, secretKey, region string, store *meta.Store) error {
	if store == nil {
		return errors.New("replication: store required")
	}
	if remote == "" {
		return errors.New("replication: -repl-remote required")
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
			AccessKey: accessKey,
			SecretKey: secretKey,
			Region:    region,
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

func runReplPull(remote, since string, limit int, fetchData bool, watch bool, interval, backoffMax time.Duration, accessKey, secretKey, region string, store *meta.Store, eng *engine.Engine) error {
	if eng == nil {
		return errors.New("replication: engine required")
	}
	if remote == "" {
		return errors.New("replication: -repl-remote required")
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
			AccessKey: accessKey,
			SecretKey: secretKey,
			Region:    region,
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
	for {
		lastHLC, applied, err := runReplPullOnce(ctx, client, since, limit, fetchData, eng, missingCache)
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
	}
}

func chunkKey(ch replMissingChunk) string {
	return fmt.Sprintf("%s:%d:%d", ch.SegmentID, ch.Offset, ch.Length)
}

func fetchChunkWithRetry(ctx context.Context, client *replClient, eng *engine.Engine, ch replMissingChunk, attempts int) error {
	if attempts <= 0 {
		attempts = 1
	}
	backoff := 200 * time.Millisecond
	var lastErr error
	for i := 0; i < attempts; i++ {
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

func runReplPullOnce(ctx context.Context, client *replClient, since string, limit int, fetchData bool, eng *engine.Engine, cache *replMissingCache) (string, int, error) {
	oplogResp, err := client.getOplog(since, limit)
	if err != nil {
		return "", 0, err
	}
	if len(oplogResp.Entries) == 0 {
		fmt.Println("repl: no new oplog entries")
		return oplogResp.LastHLC, 0, nil
	}
	applyResp, err := client.applyOplog(oplogResp.Entries)
	if err != nil {
		return "", 0, err
	}
	fmt.Printf("repl: applied=%d remote_last_hlc=%s\n", applyResp.Applied, oplogResp.LastHLC)

	if !fetchData {
		return oplogResp.LastHLC, applyResp.Applied, nil
	}
	missingManifests := make(map[string]struct{})
	for _, versionID := range applyResp.MissingManifests {
		if versionID != "" {
			missingManifests[versionID] = struct{}{}
		}
	}
	missingChunks := make(map[string]replMissingChunk)
	for _, ch := range applyResp.MissingChunks {
		missingChunks[chunkKey(ch)] = ch
	}
	if cache != nil {
		cache.addChunks(applyResp.MissingChunks)
		for k, v := range cache.snapshot() {
			if _, ok := missingChunks[k]; !ok {
				missingChunks[k] = v
			}
		}
	}
	fetchedManifests := 0
	for versionID := range missingManifests {
		manifestBytes, err := client.getManifest(versionID)
		if err != nil {
			return "", applyResp.Applied, err
		}
		man, err := eng.StoreManifestBytes(ctx, manifestBytes)
		if err != nil {
			return "", applyResp.Applied, err
		}
		chunks, err := eng.MissingChunks(man)
		if err != nil {
			return "", applyResp.Applied, err
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
			if err := fetchChunkWithRetry(ctx, client, eng, ch, 3); err != nil {
				return "", applyResp.Applied, err
			}
			fetched++
		}
	}
	if cache != nil && fetched == len(missingChunks) {
		cache.clear()
	}
	if fetchedManifests > 0 || fetched > 0 {
		fmt.Printf("repl: fetched manifests=%d chunks=%d\n", fetchedManifests, fetched)
	}
	return oplogResp.LastHLC, applyResp.Applied, nil
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
