package volume

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/4domm/images/internal/common"
)

func TestVolumeWriteReadDelete(t *testing.T) {
	cfg := Config{
		ServerID:         "a",
		DataDir:          t.TempDir(),
		MaxPackBytes:     1 << 20,
		SnapshotInterval: 0,
	}
	store, err := OpenStore(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()
	if err := store.CreateVolume(1, cfg.MaxPackBytes); err != nil {
		t.Fatalf("create pack: %v", err)
	}
	writeReq := common.EntryWriteRequest{
		EntryID:  7,
		Guard:    11,
		Metadata: common.DetectImageMetadata([]byte("hello"), "text/plain"),
	}
	if err := store.Replicate(1, writeReq, []byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}
	item, reader, err := store.Read(1, 7, 11)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	body, _ := io.ReadAll(reader)
	_ = reader.Close()
	if string(body) != "hello" || item.Size != 5 {
		t.Fatalf("unexpected read result: %q size=%d", string(body), item.Size)
	}
	if err := store.DeleteReplica(1, common.EntryDeleteRequest{EntryID: 7, Guard: 11}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, _, err := store.Read(1, 7, 11); err == nil {
		t.Fatal("expected not found after delete")
	}
}

func TestRecoverReplaysTailAfterSnapshot(t *testing.T) {
	cfg := Config{
		ServerID:         "a",
		DataDir:          t.TempDir(),
		MaxPackBytes:     1 << 20,
		SnapshotInterval: 0,
	}
	store, err := OpenStore(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := store.CreateVolume(1, cfg.MaxPackBytes); err != nil {
		t.Fatalf("create pack: %v", err)
	}

	write1 := common.EntryWriteRequest{
		EntryID:  1,
		Guard:    101,
		Metadata: common.DetectImageMetadata([]byte("first"), "text/plain"),
	}
	if err := store.Replicate(1, write1, []byte("first")); err != nil {
		t.Fatalf("write first: %v", err)
	}

	pack := store.packs[1]
	if err := pack.snapshotNow(); err != nil {
		t.Fatalf("snapshot now: %v", err)
	}

	write2 := common.EntryWriteRequest{
		EntryID:  2,
		Guard:    202,
		Metadata: common.DetectImageMetadata([]byte("second"), "text/plain"),
	}
	if err := store.Replicate(1, write2, []byte("second")); err != nil {
		t.Fatalf("write second: %v", err)
	}

	idxPath := filepath.Join(cfg.DataDir, "00000001.idx")
	raw, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if string(raw) == "" {
		t.Fatal("expected non-empty snapshot")
	}

	pack.mu.Lock()
	if err := pack.file.Close(); err != nil {
		pack.mu.Unlock()
		t.Fatalf("close pack file: %v", err)
	}
	pack.mu.Unlock()

	reopened, err := OpenStore(cfg)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Fatalf("close reopened store: %v", err)
		}
	}()

	item, reader, err := reopened.Read(1, 2, 202)
	if err != nil {
		t.Fatalf("read replayed tail entry: %v", err)
	}
	body, _ := io.ReadAll(reader)
	_ = reader.Close()
	if item.Size != uint64(len("second")) || string(body) != "second" {
		t.Fatalf("unexpected replayed entry: size=%d body=%q", item.Size, string(body))
	}
}
