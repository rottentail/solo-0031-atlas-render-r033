package persist

import (
	"atlas-render/internal/model"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// v1Artifact reproduces the legacy on-disk shape, including inline PNG.
type v1Artifact struct {
	ID     string `json:"id"`
	JobID  string `json:"jobId"`
	State  string `json:"state"`
	Digest string `json:"digest"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
	PNG    []byte `json:"png,omitempty"`
}

type v1State struct {
	Version   int                       `json:"version"`
	Scenes    map[string]model.Scene    `json:"scenes"`
	Assets    map[string]model.Asset    `json:"assets"`
	Snapshots map[string]model.Snapshot `json:"snapshots"`
	Jobs      map[string]model.Job      `json:"jobs"`
	Artifacts map[string]v1Artifact     `json:"artifacts"`
	Receipts  map[string]model.Receipt  `json:"receipts"`
	Events    []model.Event             `json:"events"`
	Sequence  int                       `json:"sequence"`
}

func testDocument() model.Document {
	return model.Document{Width: 4, Height: 4, Background: "#112233", Layers: []model.Layer{{
		ID: "plate", Kind: "rectangle", Width: 4, Height: 4,
		Transform: model.Transform{ScaleX: 1, ScaleY: 1}, Opacity: 1,
		Fill: model.Paint{Kind: "solid", Color: "#445566"},
	}}}
}

// writeLegacyState builds a genuine version-1 state containing two live
// results (sharing identical PNG bytes), one already-purged result, and the
// matching succeeded jobs and snapshot closure.
func writeLegacyState(t *testing.T, path string, sharedPNG, purgedPNG []byte) (liveA, liveB, purged string) {
	t.Helper()
	now := time.Now().UTC()
	doc := testDocument()
	snap := model.Snapshot{ID: "snap-00000001", SceneID: "scene-one", Revision: 1, State: "active", Document: doc, Assets: map[string]model.Asset{}, CreatedAt: now}
	snap.Digest = model.SnapshotDigest(snap)
	mkJob := func(id, artifact string) model.Job {
		return model.Job{ID: id, SnapshotID: snap.ID, State: "succeeded", Scale: 1, Attempt: 1, ArtifactID: artifact, CreatedAt: now, UpdatedAt: now, Payload: snap}
	}
	jobA, jobB, jobP := "job-00000001", "job-00000002", "job-00000003"
	liveA, liveB, purged = "png-00000001", "png-00000002", "png-00000003"
	st := v1State{
		Version:   model.LegacyInlineVersion,
		Scenes:    map[string]model.Scene{"scene-one": {ID: "scene-one", Name: "n", Revision: 1, State: "active", Document: doc, UpdatedAt: now}},
		Assets:    map[string]model.Asset{},
		Snapshots: map[string]model.Snapshot{snap.ID: snap},
		Jobs: map[string]model.Job{
			jobA: mkJob(jobA, liveA),
			jobB: mkJob(jobB, liveB),
			jobP: mkJob(jobP, purged),
		},
		Artifacts: map[string]v1Artifact{
			liveA:  {ID: liveA, JobID: jobA, State: "live", Digest: model.Digest(sharedPNG), Width: 4, Height: 4, PNG: sharedPNG},
			liveB:  {ID: liveB, JobID: jobB, State: "live", Digest: model.Digest(sharedPNG), Width: 4, Height: 4, PNG: sharedPNG},
			purged: {ID: purged, JobID: jobP, State: "purged", Digest: model.Digest(purgedPNG), Width: 4, Height: 4},
		},
		Receipts: map[string]model.Receipt{},
		Events:   []model.Event{},
		Sequence: 3,
	}
	b, e := json.Marshal(st)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(path, b, 0600); e != nil {
		t.Fatal(e)
	}
	return
}

func TestOpenMigratesInlineResultsLiveAndPurged(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	shared := []byte("\x89PNG\r\n\x1a\n shared identical output")
	purgedBytes := []byte("\x89PNG\r\n\x1a\n purged output bytes")
	liveA, liveB, purged := writeLegacyState(t, statePath, shared, purgedBytes)

	// The legacy state visibly embeds PNG bytes and is version 1.
	raw, _ := os.ReadFile(statePath)
	var probe map[string]any
	if e := json.Unmarshal(raw, &probe); e != nil {
		t.Fatal(e)
	}
	if probe["version"].(float64) != 1 {
		t.Fatal("fixture must be a version-1 state")
	}

	s, e := Open(statePath)
	if e != nil {
		t.Fatalf("migration failed: %v", e)
	}
	defer s.Close()

	got := s.Read()
	if got.Version != model.FormatVersion {
		t.Fatalf("state version = %d, want %d", got.Version, model.FormatVersion)
	}
	// No result bytes remain embedded in the persisted JSON.
	persisted, e := os.ReadFile(statePath)
	if e != nil {
		t.Fatal(e)
	}
	if json.Valid(persisted) == false {
		t.Fatal("migrated state is not valid JSON")
	}
	var migrated struct {
		Artifacts map[string]map[string]any `json:"artifacts"`
	}
	if e = json.Unmarshal(persisted, &migrated); e != nil {
		t.Fatal(e)
	}
	for id, rec := range migrated.Artifacts {
		if _, present := rec["png"]; present {
			t.Fatalf("artifact %s still embeds png after migration", id)
		}
	}
	// Both live results read back byte-identical, sharing one content blob.
	for _, id := range []string{liveA, liveB} {
		data, e := s.Results().Read(got.Artifacts[id].Digest)
		if e != nil {
			t.Fatalf("live artifact %s unreadable: %v", id, e)
		}
		if string(data) != string(shared) {
			t.Fatalf("artifact %s bytes changed during migration", id)
		}
	}
	entries, _ := os.ReadDir(statePath + ".results")
	blobs := 0
	for _, entry := range entries {
		if !entry.IsDir() && len(entry.Name()) == 64 {
			blobs++
		}
	}
	if blobs != 1 {
		t.Fatalf("identical outputs should share one blob, found %d", blobs)
	}
	// The previously purged result stays purged, needs no blob, and its job
	// remains a succeeded historical fact.
	if got.Artifacts[purged].State != "purged" {
		t.Fatal("purged artifact did not migrate as purged")
	}
	if got.Jobs["job-00000003"].State != "succeeded" || got.Jobs["job-00000003"].ArtifactID != purged {
		t.Fatal("purged result must not change its job facts")
	}
	if _, ok, _ := s.Results().Has(got.Artifacts[purged].Digest); ok {
		t.Fatal("purged digest should have no backing blob")
	}
	// Result byte accounting counts the single retained blob once.
	bytes, e := s.ResultBytes()
	if e != nil {
		t.Fatal(e)
	}
	if bytes != int64(len(shared)) {
		t.Fatalf("result bytes = %d, want %d (shared blob counted once)", bytes, len(shared))
	}
}

func TestOpenFailsWhenLiveBlobIsMissingOrTampered(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	shared := []byte("\x89PNG\r\n\x1a\n output")
	writeLegacyState(t, statePath, shared, []byte("old"))

	s, e := Open(statePath)
	if e != nil {
		t.Fatal(e)
	}
	digest := s.Read().Artifacts["png-00000001"].Digest
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	// Delete the blob a live artifact depends on: startup must refuse rather
	// than expose a download that cannot produce bytes.
	if e = os.Remove(filepath.Join(statePath+".results", digest)); e != nil {
		t.Fatal(e)
	}
	if _, e = Open(statePath); e == nil {
		t.Fatal("expected startup failure for missing live blob")
	}
}

func TestUpdateRejectsCommitWithoutBackingBlob(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	shared := []byte("\x89PNG\r\n\x1a\n output")
	writeLegacyState(t, statePath, shared, []byte("old"))
	s, e := Open(statePath)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	// Forge a state transition that would publish a result whose blob was
	// never written; the transaction must be rejected and memory kept intact.
	e = s.Update(func(st *model.State) error {
		snap := st.Snapshots["snap-00000001"]
		now := time.Now().UTC()
		job := model.Job{ID: "job-00000009", SnapshotID: snap.ID, State: "succeeded", Scale: 1, Attempt: 1, ArtifactID: "png-00000009", CreatedAt: now, UpdatedAt: now, Payload: snap}
		st.Jobs[job.ID] = job
		st.Artifacts[job.ArtifactID] = model.Artifact{ID: job.ArtifactID, JobID: job.ID, State: "live", Digest: model.Digest([]byte("never written")), Width: 4, Height: 4}
		st.Emit("job.succeeded", job.ID)
		return nil
	})
	if e == nil {
		t.Fatal("expected commit referencing a missing blob to fail")
	}
	if _, ok := s.Read().Artifacts["png-00000009"]; ok {
		t.Fatal("failed commit must not become visible in memory")
	}
}

func TestStartupReconcilesOrphanBlobsLeftByCrash(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	shared := []byte("\x89PNG\r\n\x1a\n output")
	writeLegacyState(t, statePath, shared, []byte("old"))
	s, e := Open(statePath)
	if e != nil {
		t.Fatal(e)
	}
	// Simulate a crash after Put succeeded but before the state transaction
	// committed: the blob exists while no artifact names it.
	orphan, e := s.Results().Put([]byte("\x89PNG\r\n\x1a\n orphaned after crash"))
	if e != nil {
		t.Fatal(e)
	}
	if _, ok, _ := s.Results().Has(orphan); !ok {
		t.Fatal("orphan blob should exist before reconciliation")
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s2, e := Open(statePath)
	if e != nil {
		t.Fatal(e)
	}
	defer s2.Close()
	if _, ok, _ := s2.Results().Has(orphan); ok {
		t.Fatal("orphan blob was not reconciled on restart")
	}
	// The legitimate shared blob survives reconciliation.
	if _, e = s2.Results().Read(s2.Read().Artifacts["png-00000001"].Digest); e != nil {
		t.Fatalf("live blob lost during reconciliation: %v", e)
	}
}

func TestReleaseUnreferencedResultsKeepsSharedContent(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	shared := []byte("\x89PNG\r\n\x1a\n shared output")
	writeLegacyState(t, statePath, shared, []byte("old"))
	s, e := Open(statePath)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	// Purge one of the two jobs sharing the blob; bytes must remain because
	// the other job is still live.
	e = s.Update(func(st *model.State) error {
		a := st.Artifacts["png-00000001"]
		a.State = "purged"
		st.Artifacts[a.ID] = a
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	if e = s.ReleaseUnreferencedResults(); e != nil {
		t.Fatal(e)
	}
	digest := s.Read().Artifacts["png-00000002"].Digest
	if _, ok, _ := s.Results().Has(digest); !ok {
		t.Fatal("purge of one job deleted bytes another job still uses")
	}
	// Purging the last user releases the blob.
	e = s.Update(func(st *model.State) error {
		a := st.Artifacts["png-00000002"]
		a.State = "purged"
		st.Artifacts[a.ID] = a
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	if e = s.ReleaseUnreferencedResults(); e != nil {
		t.Fatal(e)
	}
	if _, ok, _ := s.Results().Has(digest); ok {
		t.Fatal("blob should be released once no live artifact references it")
	}
}

func TestMigrationRejectsCorruptInlineBytes(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	shared := []byte("\x89PNG\r\n\x1a\n shared output")
	writeLegacyState(t, statePath, shared, []byte("old"))
	// Tamper with the embedded bytes while keeping the recorded digest: the
	// migration must detect the mismatch and leave the original state alone.
	raw, e := os.ReadFile(statePath)
	if e != nil {
		t.Fatal(e)
	}
	var probe map[string]any
	if e = json.Unmarshal(raw, &probe); e != nil {
		t.Fatal(e)
	}
	artifacts := probe["artifacts"].(map[string]any)
	artifacts["png-00000001"].(map[string]any)["png"] = encodeBase64([]byte("tampered inline payload"))
	raw, e = json.Marshal(probe)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(statePath, raw, 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = Open(statePath); e == nil {
		t.Fatal("migration must fail when inline bytes do not match the digest")
	}
	// The v1 state must remain untouched after a failed migration.
	after, _ := os.ReadFile(statePath)
	var still map[string]any
	if e = json.Unmarshal(after, &still); e != nil {
		t.Fatal(e)
	}
	if still["version"].(float64) != float64(model.LegacyInlineVersion) {
		t.Fatal("failed migration must not rewrite the state version")
	}
	if _, present := still["artifacts"].(map[string]any)["png-00000001"].(map[string]any)["png"]; !present {
		t.Fatal("failed migration must leave the original inline state in place")
	}
}

func TestMigrationRejectsLiveResultWithoutBytes(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	shared := []byte("\x89PNG\r\n\x1a\n shared output")
	writeLegacyState(t, statePath, shared, []byte("old"))
	raw, e := os.ReadFile(statePath)
	if e != nil {
		t.Fatal(e)
	}
	var probe map[string]any
	if e = json.Unmarshal(raw, &probe); e != nil {
		t.Fatal(e)
	}
	delete(probe["artifacts"].(map[string]any)["png-00000001"].(map[string]any), "png")
	if raw, e = json.Marshal(probe); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(statePath, raw, 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = Open(statePath); e == nil {
		t.Fatal("migration must fail when a live result has no inline bytes")
	}
}

func TestMigrationResumesAfterCrashBetweenBlobWriteAndStateRewrite(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	shared := []byte("\x89PNG\r\n\x1a\n shared output")
	liveA, liveB, _ := writeLegacyState(t, statePath, shared, []byte("old"))

	// Simulate a process killed after blobs were written but before the
	// version-2 state rewrite: the v1 state is intact while one blob exists.
	resultsDir := statePath + ".results"
	if e := os.MkdirAll(resultsDir, 0700); e != nil {
		t.Fatal(e)
	}
	digest := model.Digest(shared)
	if e := os.WriteFile(filepath.Join(resultsDir, digest), shared, 0600); e != nil {
		t.Fatal(e)
	}
	pre, _ := os.ReadFile(statePath)
	var preState map[string]any
	if e := json.Unmarshal(pre, &preState); e != nil {
		t.Fatal(e)
	}
	if preState["version"].(float64) != 1 {
		t.Fatal("fixture must still be v1 when the crash occurred")
	}

	s, e := Open(statePath)
	if e != nil {
		t.Fatalf("migration must resume from the partial run: %v", e)
	}
	defer s.Close()
	got := s.Read()
	if got.Version != model.FormatVersion {
		t.Fatalf("version = %d, want %d", got.Version, model.FormatVersion)
	}
	// The pre-existing blob is reused and both live results resolve to it.
	for _, id := range []string{liveA, liveB} {
		data, err := s.Results().Read(got.Artifacts[id].Digest)
		if err != nil {
			t.Fatalf("artifact %s unreadable after resumed migration: %v", id, err)
		}
		if string(data) != string(shared) {
			t.Fatalf("artifact %s bytes differ", id)
		}
	}
	if blobs := countBlobs(t, resultsDir); blobs != 1 {
		t.Fatalf("expected one reused blob, found %d", blobs)
	}
	// Opening the now-v2 state again is a normal startup, not another migration.
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s2, e := Open(statePath)
	if e != nil {
		t.Fatal(e)
	}
	defer s2.Close()
	if s2.Read().Version != model.FormatVersion {
		t.Fatal("restart on migrated state must stay at version 2")
	}
}

func encodeBase64(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func countBlobs(t *testing.T, dir string) int {
	t.Helper()
	entries, e := os.ReadDir(dir)
	if e != nil {
		t.Fatal(e)
	}
	n := 0
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() && len(name) == 64 {
			n++
		}
	}
	return n
}
