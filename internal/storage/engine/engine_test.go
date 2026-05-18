package engine

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kk-code-lab/seglake/internal/meta"
	ssecrypto "github.com/kk-code-lab/seglake/internal/sse"
	"github.com/kk-code-lab/seglake/internal/storage/fs"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
)

func TestEnginePutGetRoundTrip(t *testing.T) {
	dir := t.TempDir()
	engine, err := New(Options{
		Layout: fs.NewLayout(filepath.Join(dir, "data")),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	input := bytes.Repeat([]byte("abcd"), 1024)
	manifest, result, err := engine.Put(context.Background(), bytes.NewReader(input))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if manifest.Size != int64(len(input)) {
		t.Fatalf("manifest size mismatch: %d", manifest.Size)
	}
	if result.ETag == "" {
		t.Fatalf("expected ETag")
	}

	reader, gotManifest, err := engine.Get(context.Background(), result.VersionID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer func() { _ = reader.Close() }()

	if gotManifest.VersionID != manifest.VersionID {
		t.Fatalf("manifest id mismatch")
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, input) {
		t.Fatalf("data mismatch")
	}
}

func TestEnginePutObjectRecordsMetadata(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open meta: %v", err)
	}
	defer func() { _ = store.Close() }()

	engine, err := New(Options{
		Layout:    fs.NewLayout(filepath.Join(dir, "data")),
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, result, err := engine.PutObject(context.Background(), "bucket1", "key1", "", bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	got, err := store.CurrentVersion(context.Background(), "bucket1", "key1")
	if err != nil {
		t.Fatalf("CurrentVersion: %v", err)
	}
	if got != result.VersionID {
		t.Fatalf("version mismatch: %s", got)
	}

	reader, _, err := engine.GetObject(context.Background(), "bucket1", "key1")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(data, []byte("hello")) {
		t.Fatalf("data mismatch")
	}
}

func TestEngineGetRange(t *testing.T) {
	dir := t.TempDir()
	engine, err := New(Options{
		Layout: fs.NewLayout(filepath.Join(dir, "data")),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	input := bytes.Repeat([]byte("abcd"), 8)
	_, result, err := engine.Put(context.Background(), bytes.NewReader(input))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	reader, _, err := engine.GetRange(context.Background(), result.VersionID, 3, 7)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	defer func() { _ = reader.Close() }()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != string(input[3:10]) {
		t.Fatalf("range mismatch: %q", string(got))
	}
}

func TestEngineSSES3PutGetRangeAndTamper(t *testing.T) {
	dir := t.TempDir()
	key := ssecrypto.Key{ID: "local:v1"}
	copy(key.Bytes[:], bytes.Repeat([]byte{7}, 32))
	provider, err := ssecrypto.NewProvider(key.ID, []ssecrypto.Key{key})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	engine, err := New(Options{
		Layout: fs.NewLayout(filepath.Join(dir, "data")),
		SSE:    provider,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	input := []byte("hello encrypted world")
	man, result, err := engine.PutObjectSSES3(context.Background(), "bucket", "key", "", bytes.NewReader(input))
	if err != nil {
		t.Fatalf("PutObjectSSES3: %v", err)
	}
	if !man.Encrypted() {
		t.Fatalf("expected encrypted manifest")
	}
	raw, err := os.ReadFile(engine.Layout().SegmentPath(man.Chunks[0].SegmentID))
	if err != nil {
		t.Fatalf("ReadFile segment: %v", err)
	}
	if bytes.Contains(raw, input) {
		t.Fatalf("segment contains plaintext")
	}

	reader, _, err := engine.Get(context.Background(), result.VersionID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, input) {
		t.Fatalf("plaintext mismatch")
	}

	rangeReader, _, err := engine.GetRange(context.Background(), result.VersionID, 6, 9)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	rangeGot, err := io.ReadAll(rangeReader)
	_ = rangeReader.Close()
	if err != nil {
		t.Fatalf("ReadAll range: %v", err)
	}
	if string(rangeGot) != string(input[6:15]) {
		t.Fatalf("range mismatch: %q", rangeGot)
	}

	segPath := engine.Layout().SegmentPath(man.Chunks[0].SegmentID)
	file, err := os.OpenFile(segPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := file.WriteAt([]byte{0xff}, man.Chunks[0].Offset); err != nil {
		_ = file.Close()
		t.Fatalf("WriteAt: %v", err)
	}
	_ = file.Close()
	badReader, _, err := engine.Get(context.Background(), result.VersionID)
	if err != nil {
		t.Fatalf("Get tampered: %v", err)
	}
	if _, err := io.ReadAll(badReader); err == nil {
		_ = badReader.Close()
		t.Fatalf("expected authentication failure")
	}
	_ = badReader.Close()
}

func TestEngineSSES3WithVaultTransitProvider(t *testing.T) {
	var dek [32]byte
	for i := range dek {
		dek[i] = byte(i + 1)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/v1/transit/datakey/plaintext/seglake-test"):
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{
				"plaintext":  base64.StdEncoding.EncodeToString(dek[:]),
				"ciphertext": "vault:v1:seglake-test:test",
			}})
		case strings.HasPrefix(r.URL.Path, "/v1/transit/decrypt/seglake-test"):
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{
				"plaintext": base64.StdEncoding.EncodeToString(dek[:]),
			}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	provider, err := ssecrypto.NewVaultTransitProvider(ssecrypto.VaultTransitConfig{
		Address:   server.URL,
		Mount:     "transit",
		Token:     "test-token",
		ActiveKey: "seglake-test",
	})
	if err != nil {
		t.Fatalf("NewVaultTransitProvider: %v", err)
	}
	engine, err := New(Options{
		Layout: fs.NewLayout(filepath.Join(t.TempDir(), "data")),
		SSE:    provider,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	input := []byte("vault backed object")
	man, result, err := engine.PutObjectSSES3(context.Background(), "bucket", "key", "", bytes.NewReader(input))
	if err != nil {
		t.Fatalf("PutObjectSSES3: %v", err)
	}
	if man.Encryption == nil || man.Encryption.WrapAlgorithm != ssecrypto.WrapVaultTransitV1 {
		t.Fatalf("wrap algorithm = %+v", man.Encryption)
	}
	if len(man.Encryption.Keys) != 1 || len(man.Encryption.Keys[0].WrapNonce) != 0 {
		t.Fatalf("unexpected vault key entry: %+v", man.Encryption.Keys)
	}
	reader, _, err := engine.Get(context.Background(), result.VersionID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, input) {
		t.Fatalf("plaintext mismatch")
	}
}

func TestEngineGetManifestFallbackToWalk(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open meta: %v", err)
	}
	defer func() { _ = store.Close() }()

	layout := fs.NewLayout(filepath.Join(dir, "data"))
	engine, err := New(Options{
		Layout:    layout,
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	man := &manifest.Manifest{
		Bucket:    "bucket",
		Key:       "key",
		VersionID: "v1",
	}
	manifestPath := layout.ManifestPath(formatManifestName(man.Bucket, man.Key, man.VersionID))
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := writeManifestFile(manifestPath, &manifest.BinaryCodec{}, man); err != nil {
		t.Fatalf("writeManifestFile: %v", err)
	}
	stalePath := layout.ManifestPath("missing-" + man.VersionID)
	if err := store.RecordManifest(context.Background(), man.VersionID, stalePath); err != nil {
		t.Fatalf("RecordManifest: %v", err)
	}

	got, err := engine.GetManifest(context.Background(), man.VersionID)
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if got.Bucket != man.Bucket || got.Key != man.Key || got.VersionID != man.VersionID {
		t.Fatalf("manifest mismatch: %+v", got)
	}
}

func TestEngineGetManifestMissingReturnsNotExist(t *testing.T) {
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatalf("Open meta: %v", err)
	}
	defer func() { _ = store.Close() }()

	layout := fs.NewLayout(filepath.Join(dir, "data"))
	engine, err := New(Options{
		Layout:    layout,
		MetaStore: store,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	versionID := "missing-v1"
	stalePath := layout.ManifestPath(versionID)
	if err := store.RecordManifest(context.Background(), versionID, stalePath); err != nil {
		t.Fatalf("RecordManifest: %v", err)
	}

	_, err = engine.GetManifest(context.Background(), versionID)
	if err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected not exist error, got %v", err)
	}
}

func TestEngineManifestNameEscapesKey(t *testing.T) {
	layout := fs.NewLayout(t.TempDir())
	name := formatManifestName("bucket", "../escape", "v1")
	if strings.ContainsRune(name, os.PathSeparator) {
		t.Fatalf("manifest name should not include path separators: %q", name)
	}
	path := layout.ManifestPath(name)
	if filepath.Dir(path) != layout.ManifestsDir {
		t.Fatalf("manifest path escaped manifests dir: %q", path)
	}
}

func TestMissingChunksDetectsHashMismatch(t *testing.T) {
	dir := t.TempDir()
	layout := fs.NewLayout(filepath.Join(dir, "data"))
	engine, err := New(Options{Layout: layout})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	payload := bytes.Repeat([]byte("abcd"), 512)
	man, _, err := engine.Put(context.Background(), bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if len(man.Chunks) == 0 {
		t.Fatalf("expected chunks")
	}
	ch := man.Chunks[0]
	path := layout.SegmentPath(ch.SegmentID)
	file, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	buf := []byte{0}
	if _, err := file.ReadAt(buf, ch.Offset); err != nil && err != io.EOF {
		_ = file.Close()
		t.Fatalf("read segment: %v", err)
	}
	buf[0] ^= 0xff
	if _, err := file.WriteAt(buf, ch.Offset); err != nil {
		_ = file.Close()
		t.Fatalf("write segment: %v", err)
	}
	_ = file.Close()

	missing, err := engine.MissingChunks(man)
	if err != nil {
		t.Fatalf("MissingChunks: %v", err)
	}
	if len(missing) == 0 {
		t.Fatalf("expected missing chunk after corruption")
	}
}
