//go:build crashharness

package ops

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kk-code-lab/seglake/internal/storage/segment"
)

const (
	crashAccessKey = "test"
	crashSecretKey = "testsecret"
	crashRegion    = "us-east-1"
)

type mpuInitResult struct {
	UploadID string `xml:"UploadId"`
}

type mpuCompleteRequest struct {
	XMLName xml.Name          `xml:"CompleteMultipartUpload"`
	Parts   []mpuCompletePart `xml:"Part"`
}

type mpuCompletePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

func TestCrashHarness(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	root := filepath.Clean(filepath.Join(wd, "..", ".."))
	dataDir := t.TempDir()
	bin := filepath.Join(t.TempDir(), "seglake")

	build := exec.Command("go", "build", "-o", bin, "./cmd/seglake")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, string(out))
	}

	addr, err := pickFreePort()
	if err != nil {
		t.Fatalf("pickFreePort: %v", err)
	}
	host := "http://" + addr

	iters := parseIterations(t)
	client := &http.Client{Timeout: 5 * time.Second}
	for i := 0; i < iters; i++ {
		cmd := startServerWithBarrier(t, bin, dataDir, addr)
		t.Cleanup(func() {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
				_, _ = cmd.Process.Wait()
			}
		})
		if err := waitForStats(client, host, 2*time.Second); err != nil {
			_ = cmd.Process.Kill()
			t.Fatalf("server did not start: %v", err)
		}

		keyBase := fmt.Sprintf("demo/iter-%d", i+1)
		putObject(t, client, host, keyBase+"/small.txt", []byte("hello"))
		putObject(t, client, host, keyBase+"/large.bin", bytes.Repeat([]byte("a"), 5<<20))
		multipartUpload(t, client, host, keyBase+"/mpu.bin", bytes.Repeat([]byte("a"), 5<<20), []byte("tail"))

		stopServerGracefully(cmd, 2*time.Second)

		if parseCorrupt(t) {
			if err := corruptFirstSegment(filepath.Join(dataDir, "objects", "segments")); err != nil {
				t.Fatalf("corrupt segment: %v", err)
			}
		}

		// Restart once to seal open segments before fsck/rebuild.
		cmd = startServerWithBarrier(t, bin, dataDir, addr)
		t.Cleanup(func() {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
				_, _ = cmd.Process.Wait()
			}
		})
		if err := waitForStats(client, host, 2*time.Second); err != nil {
			_ = cmd.Process.Kill()
			t.Fatalf("server did not restart for seal: %v", err)
		}
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()

		fsck := runOpsJSON(t, bin, dataDir, "fsck")
		assertReportOK(t, "fsck", fsck)
		rebuild := runOpsJSON(t, bin, dataDir, "rebuild-index")
		assertReportOK(t, "rebuild-index", rebuild)
		if parseCorrupt(t) {
			scrub := runOpsJSON(t, bin, dataDir, "scrub")
			if int64Value(scrub["errors"]) == 0 {
				t.Fatalf("expected scrub errors after corruption")
			}
		}

		cmd = startServerWithBarrier(t, bin, dataDir, addr)
		t.Cleanup(func() {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
				_, _ = cmd.Process.Wait()
			}
		})
		if err := waitForStats(client, host, 2*time.Second); err != nil {
			_ = cmd.Process.Kill()
			t.Fatalf("server did not restart: %v", err)
		}
		if parseCorrupt(t) {
			getObjectExpectStatus(t, client, host, keyBase+"/small.txt", http.StatusInternalServerError)
		} else {
			getObjectWithBody(t, client, host, keyBase+"/small.txt", "hello")
		}
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}
}

func startServerWithBarrier(t *testing.T, bin, dataDir, addr string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(bin,
		"-addr", addr,
		"-data-dir", dataDir,
		"-sync-interval", "5ms",
		"-sync-bytes", "1048576",
		"-access-key", crashAccessKey,
		"-secret-key", crashSecretKey,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	return cmd
}

func putObject(t *testing.T, client *http.Client, host, key string, data []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, host+"/"+key, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(req, crashAccessKey, crashSecretKey, crashRegion)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("PUT error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: %d", resp.StatusCode)
	}
}

func getObjectWithBody(t *testing.T, client *http.Client, host, key, want string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, host+"/"+key, nil)
	if err != nil {
		t.Fatalf("GET request: %v", err)
	}
	signRequest(req, crashAccessKey, crashSecretKey, crashRegion)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != want {
		t.Fatalf("body mismatch: got %q want %q", string(body), want)
	}
}

func getObjectExpectStatus(t *testing.T, client *http.Client, host, key string, status int) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, host+"/"+key, nil)
	if err != nil {
		t.Fatalf("GET request: %v", err)
	}
	signRequest(req, crashAccessKey, crashSecretKey, crashRegion)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != status {
		t.Fatalf("GET status: %d", resp.StatusCode)
	}
}

func multipartUpload(t *testing.T, client *http.Client, host, key string, part1, part2 []byte) {
	t.Helper()
	initReq, err := http.NewRequest(http.MethodPost, host+"/"+key+"?uploads", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(initReq, crashAccessKey, crashSecretKey, crashRegion)
	initResp, err := client.Do(initReq)
	if err != nil {
		t.Fatalf("init error: %v", err)
	}
	initBody, _ := io.ReadAll(initResp.Body)
	initResp.Body.Close()
	if initResp.StatusCode != http.StatusOK {
		t.Fatalf("init status: %d", initResp.StatusCode)
	}
	var initResult mpuInitResult
	if err := xml.Unmarshal(initBody, &initResult); err != nil {
		t.Fatalf("init decode: %v", err)
	}
	if initResult.UploadID == "" {
		t.Fatalf("missing upload id")
	}

	etag1 := uploadPart(t, client, host, key, initResult.UploadID, 1, part1)
	etag2 := uploadPart(t, client, host, key, initResult.UploadID, 2, part2)

	complete := mpuCompleteRequest{
		Parts: []mpuCompletePart{
			{PartNumber: 1, ETag: etag1},
			{PartNumber: 2, ETag: etag2},
		},
	}
	var buf bytes.Buffer
	if err := xml.NewEncoder(&buf).Encode(complete); err != nil {
		t.Fatalf("encode complete: %v", err)
	}
	completeReq, err := http.NewRequest(http.MethodPost, host+"/"+key+"?uploadId="+initResult.UploadID, &buf)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(completeReq, crashAccessKey, crashSecretKey, crashRegion)
	completeResp, err := client.Do(completeReq)
	if err != nil {
		t.Fatalf("complete error: %v", err)
	}
	completeResp.Body.Close()
	if completeResp.StatusCode != http.StatusOK {
		t.Fatalf("complete status: %d", completeResp.StatusCode)
	}
}

func uploadPart(t *testing.T, client *http.Client, host, key, uploadID string, partNumber int, data []byte) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/%s?partNumber=%d&uploadId=%s", host, key, partNumber, uploadID), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	signRequest(req, crashAccessKey, crashSecretKey, crashRegion)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("PUT part error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT part status: %d", resp.StatusCode)
	}
	return resp.Header.Get("ETag")
}

func runOpsJSON(t *testing.T, bin, dataDir, mode string) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "-mode", mode, "-data-dir", dataDir, "-json")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("ops %s failed: %v\n%s", mode, err, string(out))
	}
	var report map[string]any
	if err := json.Unmarshal(out, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	return report
}

func assertReportOK(t *testing.T, mode string, report map[string]any) {
	t.Helper()
	errors := int64Value(report["errors"])
	invalid := int64Value(report["invalid_manifests"])
	missing := int64Value(report["missing_segments"])
	oob := int64Value(report["out_of_bounds_chunks"])
	if errors != 0 || invalid != 0 || missing != 0 || oob != 0 {
		t.Fatalf("report %s not clean: errors=%d invalid=%d missing=%d oob=%d", mode, errors, invalid, missing, oob)
	}
}

func int64Value(v any) int64 {
	switch t := v.(type) {
	case float64:
		return int64(t)
	case int64:
		return t
	case int:
		return int64(t)
	default:
		return 0
	}
}

func waitForStats(client *http.Client, host string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	url := host + "/v1/meta/stats"
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		signRequest(req, crashAccessKey, crashSecretKey, crashRegion)
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for stats")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func parseIterations(t *testing.T) int {
	t.Helper()
	raw := os.Getenv("CRASH_ITER")
	if raw == "" {
		return 1
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		t.Fatalf("invalid CRASH_ITER=%q", raw)
	}
	return n
}

func parseCorrupt(t *testing.T) bool {
	t.Helper()
	raw := os.Getenv("CRASH_CORRUPT")
	return raw == "1" || raw == "true" || raw == "yes"
}

func corruptFirstSegment(dir string) error {
	var target string
	errStop := errors.New("stop-walk")
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		target = path
		return errStop
	})
	if err != nil && err != errStop {
		return err
	}
	if target == "" {
		return fmt.Errorf("no segment files to corrupt")
	}
	f, err := os.OpenFile(target, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	dataEnd := info.Size() - segment.FooterLen()
	offset := segment.SegmentHeaderLen() + segment.RecordHeaderLen()
	if dataEnd <= offset {
		return fmt.Errorf("segment too small to corrupt")
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], offset); err != nil {
		return err
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b[:], offset); err != nil {
		return err
	}
	return f.Sync()
}

func pickFreePort() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr, nil
}

func stopServerGracefully(cmd *exec.Cmd, timeout time.Duration) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() {
		_, _ = cmd.Process.Wait()
		close(done)
	}()
	select {
	case <-done:
		return
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}
}

func signRequest(r *http.Request, accessKey, secretKey, region string) {
	if r == nil || r.URL == nil {
		return
	}
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateScope := amzDate[:8]
	r.Header.Set("X-Amz-Date", amzDate)
	r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	r.Header.Set("Host", r.URL.Host)

	canonicalHeaders, signedHeaders := canonicalHeadersForRequest(r)
	canonicalRequest := strings.Join([]string{
		r.Method,
		canonicalURI(r),
		canonicalQueryFromURL(r.URL),
		canonicalHeaders,
		strings.Join(signedHeaders, ";"),
		"UNSIGNED-PAYLOAD",
	}, "\n")

	hash := sha256.Sum256([]byte(canonicalRequest))
	scope := dateScope + "/" + region + "/s3/aws4_request"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hex.EncodeToString(hash[:]),
	}, "\n")

	signingKey := deriveSigningKey(secretKey, dateScope, region, "s3")
	signature := hmacSHA256Hex(signingKey, stringToSign)
	auth := "AWS4-HMAC-SHA256 " +
		"Credential=" + accessKey + "/" + scope + "," +
		"SignedHeaders=" + strings.Join(signedHeaders, ";") + "," +
		"Signature=" + signature
	r.Header.Set("Authorization", auth)
}

func canonicalHeadersForRequest(r *http.Request) (string, []string) {
	headers := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	var b strings.Builder
	for _, h := range headers {
		var value string
		if h == "host" {
			value = r.Host
		} else {
			value = r.Header.Get(h)
		}
		b.WriteString(h)
		b.WriteByte(':')
		b.WriteString(value)
		b.WriteByte('\n')
	}
	return b.String(), headers
}

func canonicalQueryFromURL(u *url.URL) string {
	if u.RawQuery == "" {
		return ""
	}
	values := u.Query()
	var pairs []string
	for k, vs := range values {
		for _, v := range vs {
			pairs = append(pairs, encodeRfc3986(k)+"="+encodeRfc3986(v))
		}
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "&")
}

func canonicalURI(r *http.Request) string {
	if r.URL == nil {
		return "/"
	}
	path := r.URL.EscapedPath()
	if path == "" {
		return "/"
	}
	return path
}

func encodeRfc3986(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'):
			b.WriteByte(c)
		case c == '-' || c == '_' || c == '.' || c == '~':
			b.WriteByte(c)
		default:
			b.WriteString(fmt.Sprintf("%%%02X", c))
		}
	}
	return b.String()
}

func deriveSigningKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func hmacSHA256Hex(key []byte, data string) string {
	return hex.EncodeToString(hmacSHA256(key, data))
}
