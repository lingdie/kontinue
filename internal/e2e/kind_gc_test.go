package e2e_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/storage"
	"lukechampine.com/blake3"
)

// This opt-in test uses the deployed collector, PostgreSQL and MinIO.
func TestKindSnapshotGC(t *testing.T) {
	cluster := os.Getenv("KONTINUE_GC_KIND")
	if cluster == "" {
		t.Skip("set KONTINUE_GC_KIND to a dedicated Kind cluster")
	}
	if cluster == "kontinue-dev" {
		t.Fatal("use a separate test cluster")
	}
	command := func(stdin, name string, args ...string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, name, args...)
		cmd.Stdin = strings.NewReader(stdin)
		raw, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s failed: %v: %s", name, err, raw)
		}
		return strings.TrimSpace(string(raw))
	}
	kube := func(input string, args ...string) string {
		t.Helper()
		prefix := []string{"--context", "kind-" + cluster, "-n", "kontinue-system"}
		return command(input, "kubectl", append(prefix, args...)...)
	}
	sql := func(query string) string {
		t.Helper()
		return kube(query, "exec", "-i", "deploy/kontinue-postgres", "--", "psql", "-XAtq", "-v", "ON_ERROR_STOP=1", "-U", "kontinue", "-d", "kontinue")
	}
	ip := kube("", "get", "svc", "kontinue-minio", "-o", "jsonpath={.spec.clusterIP}")
	prefix := "http://" + ip + ":9000/kontinue/"
	object := func(method, key, data string) string {
		t.Helper()
		args := []string{"exec", "-i", cluster + "-control-plane", "curl", "--max-time", "8", "-sS", "-X", method, "-o", "/dev/null", "-w", "%{http_code}"}
		if method == "HEAD" {
			args = append(args, "--head")
		}
		if method == "PUT" {
			args = append(args, "--data-binary", "@-")
		}
		return command(data, "docker", append(args, prefix+key)...)
	}
	waitFor := func(condition string, check func() bool) {
		t.Helper()
		deadline := time.Now().Add(60 * time.Second)
		for !check() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", condition)
			}
			time.Sleep(time.Second)
		}
	}
	id := "gc-" + strings.ToLower(rand.Text()[:12])
	old, active, pending := id+"-old", id+"-active", id+"-pending"
	chunks := make([]string, 3)
	for i := range chunks {
		body := fmt.Sprintf("%s-%d", id, i)
		sum := blake3.Sum256([]byte(body))
		chunks[i] = hex.EncodeToString(sum[:])
		key := storage.ChunkKey(chunks[i])
		if code := object("PUT", key, body); code != "200" {
			t.Fatalf("store chunk HTTP %s", code)
		}
		t.Cleanup(func() { object("DELETE", key, "") })
	}
	length := int32(len(id) + 2)
	putManifest := func(snap string, hashes ...string) {
		t.Helper()
		m := protocol.SnapshotManifest{SnapshotID: snap, PagesImgTotalSize: int64(len(hashes)) * int64(length)}
		for i, h := range hashes {
			m.Chunks = append(m.Chunks, protocol.ChunkRef{Index: uint32(i), Offset: int64(i) * int64(length), Length: length, Blake3Hash: h})
		}
		body, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		key := storage.ArtifactKey(snap, protocol.ManifestFileName)
		if got := object("PUT", key, string(body)); got != "200" {
			t.Fatalf("store manifest HTTP %s", got)
		}
		t.Cleanup(func() { object("DELETE", key, "") })
	}
	row := func(snap string, obsolete bool, n int) string {
		return fmt.Sprintf(`INSERT INTO kt_snapshots (snapshot_id,kind,base_image,manifest_uri,origin_node_id,total_memory_bytes,superseded,fingerprint_json,created_at) VALUES ('%s','SESSION_SUSPEND','fixture','test','test',%d,%t,'{}',now());`, snap, n*int(length), obsolete)
	}
	putManifest(old, chunks...)
	t.Cleanup(func() {
		sql(fmt.Sprintf("DELETE FROM kt_sandboxes WHERE sandbox_id='%s'; DELETE FROM kt_snapshots WHERE snapshot_id IN ('%s','%s','%s');", id, old, active, pending))
	})
	sql("BEGIN;" + row(old, true, 3) + row(active, false, 1) + "COMMIT;")
	logContains := func(fragment string) bool {
		return strings.Contains(kube("", "logs", "deploy/kontinue-apiserver", "--since=2m", "--tail=500"), fragment)
	}
	assertIntact := func() {
		t.Helper()
		for _, h := range chunks {
			if object("HEAD", storage.ChunkKey(h), "") != "200" {
				t.Fatal("collector removed a fixture chunk early")
			}
		}
	}
	waitFor("incomplete live manifest", func() bool { return logContains("read manifest " + active) })
	assertIntact()
	// Persist the upload intent before restoring access to the other live manifest.
	sql(fmt.Sprintf(`INSERT INTO kt_sandboxes (sandbox_id,state,active_snapshot_id,updated_at,payload_json) VALUES ('%s','FAILED','',now(),'{"sandboxId":"%s","state":"FAILED","pendingSnapshotId":"%s"}');`, id, id, pending))
	putManifest(active, chunks[0])
	waitFor("unpublished snapshot", func() bool { return logContains("snapshot " + pending + " awaits publication") })
	assertIntact()
	putManifest(pending, chunks[2])
	sql("BEGIN;" + row(pending, false, 1) + fmt.Sprintf("DELETE FROM kt_sandboxes WHERE sandbox_id='%s'; COMMIT;", id))
	waitFor("collection", func() bool {
		return sql(fmt.Sprintf("SELECT count(*) FROM kt_snapshots WHERE snapshot_id='%s';", old)) == "0"
	})
	for i, h := range chunks {
		code := object("HEAD", storage.ChunkKey(h), "")
		if (i == 1 && code != "404") || (i != 1 && code != "200") {
			t.Fatalf("chunk %d HTTP %s", i, code)
		}
	}
	t.Logf("%s: missing manifest and unpublished capture preserved all chunks; exclusive chunk reclaimed, both shared chunks retained", id)
}
