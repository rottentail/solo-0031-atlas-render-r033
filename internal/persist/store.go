package persist

import (
	"atlas-render/internal/model"
	"atlas-render/internal/resultstore"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

type Store struct {
	mu      sync.RWMutex
	path    string
	dir     string
	state   model.State
	lock    *os.File
	results *resultstore.Store
	write   func(string, []byte) error
}

func Open(path string) (*Store, error) {
	dir := filepath.Dir(path)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	lock, e := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		lock.Close()
		return nil, fmt.Errorf("state is already opened by another process: %w", e)
	}
	results, e := resultstore.Open(path + ".results")
	if e != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("open result store: %w", e)
	}
	s := &Store{path: path, dir: dir, state: model.NewState(), lock: lock, results: results, write: atomicWrite}
	data, e := os.ReadFile(path)
	if e != nil && !os.IsNotExist(e) {
		s.Close()
		return nil, e
	}
	if e == nil {
		if e = json.Unmarshal(data, &s.state); e != nil {
			s.Close()
			return nil, fmt.Errorf("invalid persisted JSON: %w", e)
		}
		// Legacy format embedded result PNG bytes in the state. Move every
		// surviving byte into the result store before the state is used.
		// Purged records already carry no bytes and migrate as-is.
		if s.state.Version == model.LegacyInlineVersion {
			if e = s.migrateInlineResults(data); e != nil {
				s.Close()
				return nil, e
			}
		}
		if e = Validate(s.state); e != nil {
			s.Close()
			return nil, e
		}
		if e = s.verifyResultBlobs(); e != nil {
			s.Close()
			return nil, e
		}
		if e = s.releaseUnreferencedResults(); e != nil {
			s.Close()
			return nil, e
		}
	}
	return s, nil
}

// legacyArtifact mirrors the version-1 persisted artifact, including the
// inline PNG field that version 2 no longer maps onto model.Artifact.
type legacyArtifact struct {
	ID     string `json:"id"`
	JobID  string `json:"jobId"`
	State  string `json:"state"`
	Digest string `json:"digest"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
	PNG    []byte `json:"png,omitempty"`
}

type legacyState struct {
	Artifacts map[string]legacyArtifact `json:"artifacts"`
}

// migrateInlineResults moves inline PNG bytes from a version-1 state into
// the content-addressed result store and rewrites the state in version 2.
// The state file is only replaced once every blob is durable, so a crash
// leaves the original state intact and the migration simply restarts.
func (s *Store) migrateInlineResults(raw []byte) error {
	var legacy legacyState
	if e := json.Unmarshal(raw, &legacy); e != nil {
		return fmt.Errorf("decode legacy artifacts: %w", e)
	}
	migratedLive := 0
	migratedPurged := 0
	for id, old := range legacy.Artifacts {
		cur, ok := s.state.Artifacts[id]
		if !ok {
			continue
		}
		if cur.State == "purged" {
			migratedPurged++
			continue
		}
		if len(old.PNG) == 0 {
			return fmt.Errorf("legacy artifact %s is live without inline bytes", id)
		}
		digest, e := s.results.Put(old.PNG)
		if e != nil {
			return fmt.Errorf("migrate artifact %s: %w", id, e)
		}
		if digest != cur.Digest {
			return fmt.Errorf("legacy artifact %s digest mismatch", id)
		}
		cur.PNG = nil
		s.state.Artifacts[id] = cur
		migratedLive++
	}
	s.state.Version = model.FormatVersion
	if e := Validate(s.state); e != nil {
		return fmt.Errorf("migrated state failed validation: %w", e)
	}
	b, e := json.Marshal(s.state)
	if e != nil {
		return e
	}
	if e = s.write(s.path, b); e != nil {
		return fmt.Errorf("persist migrated state: %w", e)
	}
	return nil
}

// verifyResultBlobs confirms every live artifact has a durable, digest
// verified blob. A state record without its bytes can never be published or
// served, so startup fails rather than exposing a phantom result.
func (s *Store) verifyResultBlobs() error {
	for id, a := range s.state.Artifacts {
		if a.State != "live" {
			continue
		}
		data, e := s.results.Read(a.Digest)
		if e != nil {
			return fmt.Errorf("artifact %s: %w", id, e)
		}
		if model.Digest(data) != a.Digest {
			return fmt.Errorf("artifact %s blob digest mismatch", id)
		}
	}
	return nil
}

func (s *Store) Close() error {
	if s.lock == nil {
		return nil
	}
	_ = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
	e := s.lock.Close()
	s.lock = nil
	return e
}

// Results exposes the independent result blob store.
func (s *Store) Results() *resultstore.Store { return s.results }

func (s *Store) Read() model.State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return model.Clone(s.state)
}

func (s *Store) Update(fn func(*model.State) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := model.Clone(s.state)
	if e := fn(&next); e != nil {
		return e
	}
	if e := Validate(next); e != nil {
		return e
	}
	// Reject a commit that would publish a result whose blob is not durable.
	for id, a := range next.Artifacts {
		if a.State != "live" {
			continue
		}
		if _, ok, e := s.results.Has(a.Digest); e != nil {
			return e
		} else if !ok {
			return fmt.Errorf("artifact %s references missing result blob", id)
		}
	}
	b, e := json.Marshal(next)
	if e != nil {
		return e
	}
	if e = s.write(s.path, b); e != nil {
		return fmt.Errorf("persist state: %w", e)
	}
	s.state = next
	return nil
}

// liveDigests collects the content addresses still referenced by live
// artifacts. Identical output shared by jobs appears once.
func (s *Store) liveDigests() map[string]struct{} {
	live := map[string]struct{}{}
	for _, a := range s.state.Artifacts {
		if a.State == "live" {
			live[a.Digest] = struct{}{}
		}
	}
	return live
}

// ReleaseUnreferencedResults deletes blobs no live artifact names. It runs
// after purge commits, so a purge only frees bytes nobody still uses.
func (s *Store) ReleaseUnreferencedResults() error {
	s.mu.RLock()
	live := s.liveDigests()
	s.mu.RUnlock()
	_, e := s.results.Sweep(live)
	return e
}

func (s *Store) releaseUnreferencedResults() error {
	_, e := s.results.Sweep(s.liveDigests())
	return e
}

// ResultBytes reports the on-disk size of the blobs backing live artifacts.
func (s *Store) ResultBytes() (int64, error) {
	s.mu.RLock()
	live := s.liveDigests()
	s.mu.RUnlock()
	return s.results.LiveBytes(live)
}

func atomicWrite(path string, data []byte) error {
	f, e := os.CreateTemp(filepath.Dir(path), ".atlas-write-")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(data)
	}
	if e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e == nil {
		e = closeErr
	}
	if e != nil {
		return e
	}
	return os.Rename(name, path)
}
