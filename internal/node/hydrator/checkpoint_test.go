package hydrator

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kontinue-sh/kontinue/internal/node/arena"
	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCompletedCheckpointReplaysAfterDaemonReplacement(t *testing.T) {
	ctx := context.Background()
	guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != protocol.PathInternalHealth {
			t.Errorf("unexpected guest request %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"healthy":true,"initialized":true,"sandboxId":"box","executionId":2}`))
	}))
	defer guest.Close()
	manager, err := arena.NewManager(arena.ManagerConfig{L2CASDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	cfg := ServiceConfig{ScratchDir: t.TempDir(), NodeID: "worker", Driver: &inspectingDriver{}, Arena: manager}
	prep := protocol.PrepareCheckpointRequest{TargetSnapshotID: "snap", SandboxID: "box", SlotID: "slot", ExecutionID: 2, LifecycleID: "life", Kind: protocol.SnapshotKindLiveFork}
	saved := checkpointReceipt{RequestHash: checkpointRequestHash(&prep), Result: protocol.ExecuteCheckpointResponse{Manifest: protocol.SnapshotManifest{SnapshotID: "snap"}, ManifestURI: "local://manifest"}}
	claim := ownership{Version: 1, Phase: activeExecution, Slot: protocol.CarrierSlot{NodeID: "worker", SlotID: "slot"}, ContainerID: "kt-original", SandboxID: "box", ExecutionID: 2, LifecycleID: "life", KtdEndpoint: strings.TrimPrefix(guest.URL, "http://"), LastCheckpoint: &saved}
	if err := NewService(cfg).journal.write(claim, true); err != nil {
		t.Fatal(err)
	}
	s := NewService(cfg)
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	query := protocol.GetLatestSnapshotResultRequest{SnapshotID: "snap", Expected: &prep}
	got, err := s.GetLatestSnapshotResult(ctx, &query)
	if err != nil || !got.Found || got.ManifestURI != saved.Result.ManifestURI {
		t.Fatalf("receipt missing: %v", err)
	}
	if _, err := s.PrepareCheckpoint(ctx, &prep); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "replayed.tar")
	if _, err := s.ExecuteCheckpoint(ctx, &protocol.ExecuteCheckpointRequest{ContainerID: claim.ContainerID, KubeletLocation: path}); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h, err := tar.NewReader(f).Next()
	if err != nil || h.Name != "kontinue-descriptor.json" {
		t.Fatalf("missing descriptor: %v", err)
	}
	prep.ExecutionID++
	if _, err := s.GetLatestSnapshotResult(ctx, &query); !errors.Is(err, store.ErrStaleExecutionFence) {
		t.Fatalf("receipt accepted a different generation: %v", err)
	}
}

func TestPrepareCheckpointDoesNotOverwriteAnInFlightRequest(t *testing.T) {
	ctx := context.Background()
	s := NewService(ServiceConfig{ScratchDir: t.TempDir()})
	st := &activeSlotState{ownership: ownership{
		Phase:       activeExecution,
		Slot:        protocol.CarrierSlot{SlotID: "slot"},
		ContainerID: "container",
		SandboxID:   "box",
		ExecutionID: 2,
		LifecycleID: "life",
	}}
	s.activeSlots[st.Slot.SlotID] = st
	s.byContainer[st.ContainerID] = st

	first := &protocol.PrepareCheckpointRequest{
		SandboxID: "box", ExecutionID: 2, LifecycleID: "life", SlotID: "slot",
		TargetSnapshotID: "snapshot-one", Kind: protocol.SnapshotKindLiveFork,
	}
	if _, err := s.PrepareCheckpoint(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PrepareCheckpoint(ctx, first); err != nil {
		t.Fatalf("same checkpoint retry was rejected: %v", err)
	}
	second := *first
	second.TargetSnapshotID = "snapshot-two"
	if _, err := s.PrepareCheckpoint(ctx, &second); status.Code(err) != codes.Aborted {
		t.Fatalf("in-flight checkpoint was overwritten: %v", err)
	}

	st.LastCheckpoint = &checkpointReceipt{RequestHash: checkpointRequestHash(first)}
	if _, err := s.PrepareCheckpoint(ctx, &second); err != nil {
		t.Fatalf("completed checkpoint blocked the next request: %v", err)
	}
}

var errCheckpointInterrupted = errors.New("injected interruption after publishing the manifest")

type checkpointUploadFault struct {
	storage.Backend
	puts int
}

func (b *checkpointUploadFault) PutManifest(ctx context.Context, id string, manifest *protocol.SnapshotManifest) (string, error) {
	b.puts++
	uri, err := b.Backend.PutManifest(ctx, id, manifest)
	if err == nil {
		err = errCheckpointInterrupted
	}
	return uri, err
}

type checkpointRecordingDriver struct {
	inspectingDriver
	captures int
}

func (d *checkpointRecordingDriver) CheckpointSandboxContainer(_ context.Context, _, dir string, _ bool) error {
	d.captures++
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	for _, artifact := range []string{protocol.GVisorCheckpointImg, protocol.GVisorPagesMetaImg, protocol.GVisorPagesImg} {
		data := fmt.Sprintf("%s for capture %d", artifact, d.captures)
		if err := os.WriteFile(filepath.Join(dir, artifact), []byte(data), 0o600); err != nil {
			return err
		}
	}
	return nil
}

func TestPublishedCheckpointRecoversBeforeReceiptCommit(t *testing.T) {
	for _, failure := range []string{"complete upload", "missing remote chunk", "changed core artifact", "no staged receipt"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != protocol.PathInternalHealth {
					t.Errorf("unexpected guest operation %s", r.URL.Path)
				}
				_, _ = w.Write([]byte(`{"healthy":true,"initialized":true,"sandboxId":"box","executionId":2,"lifecycleId":"life"}`))
			}))
			defer guest.Close()
			backend, err := storage.NewLocalFSBackend(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			fault := &checkpointUploadFault{Backend: backend}
			arenaCfg := arena.ManagerConfig{L2CASDir: t.TempDir(), NodeID: "worker", RemoteStore: fault}
			manager, err := arena.NewManager(arenaCfg)
			if err != nil {
				t.Fatal(err)
			}
			defer manager.Close()
			driver := &checkpointRecordingDriver{}
			cfg := ServiceConfig{ScratchDir: t.TempDir(), NodeID: "worker", Driver: driver, Arena: manager}
			s := NewService(cfg)
			prep := protocol.PrepareCheckpointRequest{TargetSnapshotID: "snap", SandboxID: "box", SlotID: "slot", ExecutionID: 2, LifecycleID: "life", Kind: protocol.SnapshotKindLiveFork, LeaveRunning: true}
			claim := ownership{Version: 1, Phase: activeExecution, Slot: protocol.CarrierSlot{NodeID: cfg.NodeID, SlotID: prep.SlotID}, ContainerID: "kt-original", SandboxID: prep.SandboxID, ExecutionID: prep.ExecutionID, LifecycleID: prep.LifecycleID, KtdEndpoint: strings.TrimPrefix(guest.URL, "http://")}
			if err := s.journal.write(claim, true); err != nil {
				t.Fatal(err)
			}
			if err := s.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := s.PrepareCheckpoint(ctx, &prep); err != nil {
				t.Fatal(err)
			}
			execute := protocol.ExecuteCheckpointRequest{ContainerID: claim.ContainerID}
			if _, err := s.ExecuteCheckpoint(ctx, &execute); !errors.Is(err, errCheckpointInterrupted) {
				t.Fatalf("interrupt after manifest publication: %v", err)
			}
			published, err := backend.GetManifest(ctx, prep.TargetSnapshotID)
			if err != nil {
				t.Fatal(err)
			}
			claims, err := s.journal.load(cfg.NodeID)
			if err != nil || len(claims) != 1 || claims[0].StagedCheckpoint == nil || claims[0].LastCheckpoint != nil {
				t.Fatalf("missing the interrupted checkpoint window: claims=%+v err=%v", claims, err)
			}
			switch failure {
			case "missing remote chunk":
				if err := backend.DeleteBlob(ctx, storage.ChunkKey(published.Chunks[0].Blake3Hash)); err != nil {
					t.Fatal(err)
				}
			case "changed core artifact":
				data := bytes.Repeat([]byte{'x'}, int(published.CheckpointImgSize))
				key := storage.ArtifactKey(prep.TargetSnapshotID, protocol.GVisorCheckpointImg)
				if err := backend.PutBlob(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
					t.Fatal(err)
				}
			case "no staged receipt":
				if err := s.journal.write(claim, false); err != nil {
					t.Fatal(err)
				}
			}
			cfg.Arena, err = arena.NewManager(arenaCfg)
			if err != nil {
				t.Fatal(err)
			}
			defer cfg.Arena.Close()
			s = NewService(cfg)
			if err := s.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			query := protocol.GetLatestSnapshotResultRequest{SnapshotID: prep.TargetSnapshotID, Expected: &prep}
			if failure != "no staged receipt" {
				wrong := prep
				wrong.LifecycleID = "replacement"
				wrongQuery := protocol.GetLatestSnapshotResultRequest{SnapshotID: prep.TargetSnapshotID, Expected: &wrong}
				if _, err := s.GetLatestSnapshotResult(ctx, &wrongQuery); !errors.Is(err, store.ErrStaleExecutionFence) {
					t.Fatalf("accepted a different lifecycle: %v", err)
				}
				wrong = prep
				wrong.LeaveRunning = false
				if _, err := s.GetLatestSnapshotResult(ctx, &wrongQuery); !errors.Is(err, store.ErrStaleExecutionFence) {
					t.Fatalf("accepted a different checkpoint request: %v", err)
				}
			}
			got, err := s.GetLatestSnapshotResult(ctx, &query)
			switch failure {
			case "complete upload":
				if err != nil || got == nil || !got.Found || !reflect.DeepEqual(got.Manifest, *published) || got.ManifestURI != backend.ManifestURI(prep.TargetSnapshotID) {
					t.Fatalf("missing original receipt: got=%+v err=%v", got, err)
				}
				claims, err = s.journal.load(cfg.NodeID)
				if err != nil || len(claims) != 1 || claims[0].StagedCheckpoint != nil || claims[0].LastCheckpoint == nil {
					t.Fatalf("receipt was not finalized: claims=%+v err=%v", claims, err)
				}
			case "missing remote chunk", "changed core artifact":
				if err == nil {
					t.Fatal("confirmed an incomplete snapshot")
				}
			case "no staged receipt":
				if err != nil || got == nil || got.Found {
					t.Fatalf("published manifest lacked an owner-bound staging record: got=%+v err=%v", got, err)
				}
			}
			if _, err := s.PrepareCheckpoint(ctx, &prep); err != nil {
				t.Fatal(err)
			}
			replayed, err := s.ExecuteCheckpoint(ctx, &execute)
			if failure == "complete upload" {
				if err != nil || replayed == nil || !reflect.DeepEqual(replayed.Manifest, *published) {
					t.Fatalf("CRI retry did not replay the original snapshot: %v", err)
				}
			} else if err == nil {
				t.Fatal("CRI retry accepted an unconfirmed snapshot")
			}
			if driver.captures != 1 || fault.puts != 1 {
				t.Fatalf("retried capture or publication: captures=%d manifest puts=%d", driver.captures, fault.puts)
			}
		})
	}
}
