package resultstore

import (
	"atlas-render/internal/model"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPutReadIsContentAddressedAndVerified(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	data := []byte("\x89PNG\r\n\x1a\n fake render payload")
	digest, e := s.Put(data)
	if e != nil {
		t.Fatal(e)
	}
	if digest != model.Digest(data) {
		t.Fatalf("Put returned %s, want %s", digest, model.Digest(data))
	}
	got, e := s.Read(digest)
	if e != nil {
		t.Fatal(e)
	}
	if string(got) != string(data) {
		t.Fatal("Read returned different bytes")
	}
	// Putting identical content reuses the same blob; no second file appears.
	if _, e = s.Put(data); e != nil {
		t.Fatal(e)
	}
	entries, _ := os.ReadDir(s.Dir())
	if len(entries) != 1 {
		t.Fatalf("expected one shared blob, got %d entries", len(entries))
	}
}

func TestReadRejectsMissingAndTamperedBlobs(t *testing.T) {
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Read(model.Digest([]byte("absent"))); !errors.Is(e, ErrMissing) {
		t.Fatalf("expected ErrMissing, got %v", e)
	}
	data := []byte("payload that will be tampered")
	digest, _ := s.Put(data)
	if e = os.WriteFile(filepath.Join(dir, digest), []byte("different bytes"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Read(digest); e == nil {
		t.Fatal("expected digest verification failure on tampered blob")
	}
	if _, ok, _ := s.Has(digest); !ok {
		t.Fatal("Has should still report the present file")
	}
}

func TestSweepKeepsSharedBlobsAndRemovesOrphans(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	shared := []byte("identical render output")
	onlyA := []byte("render output only A still uses")
	orphan := []byte("blob left behind by a crash")
	dShared, _ := s.Put(shared)
	dA, _ := s.Put(onlyA)
	dOrphan, _ := s.Put(orphan)
	// Two jobs share dShared; job A additionally owns dA. Nothing references dOrphan.
	live := map[string]struct{}{dShared: {}, dA: {}}
	removed, e := s.Sweep(live)
	if e != nil {
		t.Fatal(e)
	}
	if removed != 1 {
		t.Fatalf("expected one orphan removed, got %d", removed)
	}
	if _, ok, _ := s.Has(dOrphan); ok {
		t.Fatal("orphan blob survived sweep")
	}
	if _, ok, _ := s.Has(dShared); !ok {
		t.Fatal("shared blob was deleted while a job still uses it")
	}
	if _, ok, _ := s.Has(dA); !ok {
		t.Fatal("referenced blob was deleted")
	}
	// A second sweep with no live references removes the rest.
	if removed, e = s.Sweep(map[string]struct{}{}); e != nil || removed != 2 {
		t.Fatalf("expected 2 removals, got %d, %v", removed, e)
	}
}

func TestOpenCleansAbandonedTempFiles(t *testing.T) {
	dir := t.TempDir()
	if e := os.WriteFile(filepath.Join(dir, ".atlas-result-stale"), []byte("partial"), 0600); e != nil {
		t.Fatal(e)
	}
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Put([]byte("complete")); e != nil {
		t.Fatal(e)
	}
	entries, _ := os.ReadDir(dir)
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == "" && len(entry.Name()) > 0 && entry.Name()[0] == '.' {
			t.Fatalf("staging file %s was not cleaned", entry.Name())
		}
	}
}
