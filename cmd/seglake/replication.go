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

func runReplPull(remote, since string, limit int, fetchData bool, accessKey, secretKey, region string, eng *engine.Engine) error {
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

	oplogResp, err := client.getOplog(since, limit)
	if err != nil {
		return err
	}
	if len(oplogResp.Entries) == 0 {
		fmt.Println("repl: no new oplog entries")
		return nil
	}
	applyResp, err := client.applyOplog(oplogResp.Entries)
	if err != nil {
		return err
	}
	fmt.Printf("repl: applied=%d remote_last_hlc=%s\n", applyResp.Applied, oplogResp.LastHLC)

	if !fetchData {
		return nil
	}
	missingManifests := make(map[string]struct{})
	for _, versionID := range applyResp.MissingManifests {
		if versionID != "" {
			missingManifests[versionID] = struct{}{}
		}
	}
	missingChunks := make(map[string]replMissingChunk)
	for _, ch := range applyResp.MissingChunks {
		key := chunkKey(ch)
		missingChunks[key] = ch
	}
	if len(missingManifests) == 0 && len(missingChunks) == 0 {
		return nil
	}
	ctx := context.Background()
	for versionID := range missingManifests {
		manifestBytes, err := client.getManifest(versionID)
		if err != nil {
			return err
		}
		man, err := eng.StoreManifestBytes(ctx, manifestBytes)
		if err != nil {
			return err
		}
		chunks, err := eng.MissingChunks(man)
		if err != nil {
			return err
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
	}
	for _, ch := range missingChunks {
		data, err := client.getChunk(ch.SegmentID, ch.Offset, ch.Length)
		if err != nil {
			return err
		}
		if err := eng.WriteSegmentRange(ctx, ch.SegmentID, ch.Offset, data); err != nil {
			return err
		}
	}
	fmt.Printf("repl: fetched manifests=%d chunks=%d\n", len(missingManifests), len(missingChunks))
	return nil
}

func chunkKey(ch replMissingChunk) string {
	return fmt.Sprintf("%s:%d:%d", ch.SegmentID, ch.Offset, ch.Length)
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
