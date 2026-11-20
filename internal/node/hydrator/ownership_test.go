package hydrator

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

func TestOwnershipClaimSurvivesServiceReplacement(t *testing.T) {
	j := ownershipJournal(filepath.Join(t.TempDir(), "ownership"))
	owner := ownership{Version: 1, Phase: preparing, Slot: protocol.CarrierSlot{SlotID: "pool/one"}, SandboxID: "session", ExecutionID: 2, ContainerID: "kt-first"}
	if err := j.write(owner, true); err != nil {
		t.Fatal(err)
	}
	other := owner
	other.ContainerID = "kt-unrelated"
	if err := j.write(other, true); !errors.Is(err, os.ErrExist) {
		t.Fatalf("second claim: %v", err)
	}
	owner.Phase = activeExecution
	owner.ViewPath = "/views/view-one"
	if err := j.write(owner, false); err != nil {
		t.Fatal(err)
	}
	path := j.path(owner.Slot.SlotID)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("journal permissions: %v (%v)", info, err)
	}
	var got ownership
	if err := json.NewDecoder(f).Decode(&got); err != nil || got != owner {
		t.Fatalf("got %+v, err %v", got, err)
	}
	if err := j.remove(owner.Slot.SlotID); err != nil {
		t.Fatal(err)
	}
	if err := j.write(other, true); err != nil {
		t.Fatalf("reuse confirmed release: %v", err)
	}
}
