package hydrator

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kontinue-sh/kontinue/internal/node/arena"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

type ownershipPhase string

const (
	preparing       ownershipPhase = "preparing"
	activeExecution ownershipPhase = "active"
	releasing       ownershipPhase = "releasing"
)

// Each file is a write-ahead claim on one carrier. The private file contains the
// guest bootstrap credential needed to resume checkpoint operations after restart.
type ownership struct {
	FailureReason  string               `json:"failureReason,omitempty"`
	View           *arena.ViewIdentity  `json:"view,omitempty"`
	Version        int                  `json:"version"`
	Phase          ownershipPhase       `json:"phase"`
	Slot           protocol.CarrierSlot `json:"slot"`
	SandboxID      string               `json:"sandboxId"`
	ExecutionID    int64                `json:"executionId"`
	LifecycleID    string               `json:"lifecycleId"`
	ContainerID    string               `json:"containerId"`
	KtdEndpoint    string               `json:"ktdEndpoint,omitempty"`
	ViewID         string               `json:"viewId,omitempty"`
	ViewPath       string               `json:"viewPath,omitempty"`
	BootstrapToken string               `json:"bootstrapToken,omitempty"`
}

type ownershipJournal string

func (j ownershipJournal) path(slotID string) string {
	return filepath.Join(string(j), fmt.Sprintf("%x.json", sha256.Sum256([]byte(slotID))))
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (j ownershipJournal) write(o ownership, create bool) error {
	if err := os.Mkdir(string(j), 0700); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
	} else if err := syncDirectory(filepath.Dir(string(j))); err != nil {
		return err
	}
	f, err := os.CreateTemp(string(j), ".pending-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = json.NewEncoder(f).Encode(o); err == nil {
		err = f.Sync()
	}
	if err = errors.Join(err, f.Close()); err != nil {
		return err
	}
	path := j.path(o.Slot.SlotID)
	if create {
		// Link is atomic and cannot overwrite an earlier daemon's unresolved claim.
		err = os.Link(f.Name(), path)
	} else {
		err = os.Rename(f.Name(), path)
	}
	if err != nil {
		return err
	}
	return syncDirectory(string(j))
}

func (j ownershipJournal) remove(slotID string) error {
	if err := os.Remove(j.path(slotID)); err != nil {
		return err
	}
	return syncDirectory(string(j))
}

func (j ownershipJournal) load(nodeID string) ([]ownership, error) {
	entries, err := os.ReadDir(string(j))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var claims []ownership
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(string(j), entry.Name()))
		if err != nil {
			return nil, err
		}
		var o ownership
		if err := json.Unmarshal(data, &o); err != nil {
			return nil, fmt.Errorf("read claim %s: %w", entry.Name(), err)
		}
		if o.Version != 1 || o.Slot.NodeID != nodeID || o.Slot.SlotID == "" || j.path(o.Slot.SlotID) != filepath.Join(string(j), entry.Name()) || o.SandboxID == "" || o.ExecutionID <= 0 || !validInstanceID(o.ContainerID) {
			return nil, fmt.Errorf("invalid ownership record %s", entry.Name())
		}
		claims = append(claims, o)
	}
	return claims, nil
}
