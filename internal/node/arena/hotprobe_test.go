package arena

import (
	"testing"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

func TestSkipThawTouchOrderPutsStackChunkFirst(t *testing.T) {
	chunks := []protocol.ChunkRef{
		{Index: 0, Offset: 0, Length: 4096, ZeroChunk: true},
		{Index: 1, Offset: 4096, Length: 4096},
		{Index: 2, Offset: 8192, Length: 4096},
		{Index: 3, Offset: 12288, Length: 4096},
	}
	got := skipThawTouchOrder(chunks)
	if len(got) != 3 {
		t.Fatalf("len=%d", len(got))
	}
	if got[0].Index != 3 || got[1].Index != 1 || got[2].Index != 2 {
		t.Fatalf("order=%v want stack-high then low-to-high", []uint32{got[0].Index, got[1].Index, got[2].Index})
	}
}
