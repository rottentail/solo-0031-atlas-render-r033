// Package resultstore keeps render result PNG bytes outside the JSON state.
//
// Blobs are content addressed by their SHA-256 digest, so identical render
// output produced by different jobs occupies a single file. A blob is only
// removed by Sweep when no live artifact names its digest, which means
// purging one job can never delete bytes another job still serves.
package resultstore

import (
	"atlas-render/internal/model"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ErrMissing reports that no blob exists for a digest.
var ErrMissing = errors.New("result blob missing")

// Store is a directory of content-addressed result blobs.
type Store struct {
	mu  sync.RWMutex
	dir string
}

// Open creates the result directory if needed and removes abandoned staging
// files left by a crash between temp-file creation and atomic replacement.
func Open(dir string) (*Store, error) {
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	s := &Store{dir: dir}
	if e := s.cleanTemp(); e != nil {
		return nil, e
	}
	return s, nil
}

// Dir reports the on-disk directory backing the store.
func (s *Store) Dir() string { return s.dir }

func validName(digest string) bool {
	if len(digest) != 64 {
		return false
	}
	for _, r := range digest {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return false
		}
	}
	return true
}

func (s *Store) path(digest string) (string, error) {
	if !validName(digest) {
		return "", fmt.Errorf("invalid result digest %q", digest)
	}
	return filepath.Join(s.dir, digest), nil
}

// Put writes data to the store under its digest. The write goes to a
// fsynced temporary file and is atomically renamed, so readers never see a
// partial blob. A blob already holding the same bytes is left untouched.
func (s *Store) Put(data []byte) (string, error) {
	digest := model.Digest(data)
	target, e := s.path(digest)
	if e != nil {
		return "", e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, err := os.ReadFile(target); err == nil {
		if model.Digest(current) == digest {
			return digest, nil
		}
		return "", fmt.Errorf("result blob %s is corrupt", digest)
	} else if !os.IsNotExist(err) {
		return "", err
	}
	tmp, e := os.CreateTemp(s.dir, ".atlas-result-")
	if e != nil {
		return "", e
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if e = tmp.Chmod(0600); e == nil {
		_, e = tmp.Write(data)
	}
	if e == nil {
		e = tmp.Sync()
	}
	if closeErr := tmp.Close(); e == nil {
		e = closeErr
	}
	if e != nil {
		return "", e
	}
	if e = os.Rename(tmpName, target); e != nil {
		return "", e
	}
	return digest, nil
}

// Read returns the bytes for digest, verifying the digest on every read so
// callers cannot serve tampered or truncated content.
func (s *Store) Read(digest string) ([]byte, error) {
	target, e := s.path(digest)
	if e != nil {
		return nil, e
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, e := os.ReadFile(target)
	if e != nil {
		if os.IsNotExist(e) {
			return nil, fmt.Errorf("%w: %s", ErrMissing, digest)
		}
		return nil, e
	}
	if model.Digest(data) != digest {
		return nil, fmt.Errorf("result blob %s failed digest verification", digest)
	}
	return data, nil
}

// Has reports whether the digest is present and how large the blob is.
func (s *Store) Has(digest string) (size int64, ok bool, err error) {
	target, e := s.path(digest)
	if e != nil {
		return 0, false, e
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, e := os.Stat(target)
	if e != nil {
		if os.IsNotExist(e) {
			return 0, false, nil
		}
		return 0, false, e
	}
	return info.Size(), true, nil
}

// Sweep deletes every blob whose digest is not in live. Blobs shared by
// several artifacts remain as long as at least one live artifact names
// their digest.
func (s *Store) Sweep(live map[string]struct{}) (removed int, err error) {
	entries, e := os.ReadDir(s.dir)
	if e != nil {
		return 0, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || strings.HasPrefix(name, ".atlas-result-") || !validName(name) {
			continue
		}
		if _, keep := live[name]; keep {
			continue
		}
		if e = os.Remove(filepath.Join(s.dir, name)); e != nil {
			return removed, e
		}
		removed++
	}
	return removed, nil
}

// LiveBytes sums the sizes of the blobs named by live.
func (s *Store) LiveBytes(live map[string]struct{}) (int64, error) {
	var total int64
	s.mu.RLock()
	defer s.mu.RUnlock()
	for digest := range live {
		target, e := s.path(digest)
		if e != nil {
			return 0, e
		}
		info, e := os.Stat(target)
		if e != nil {
			if os.IsNotExist(e) {
				return 0, fmt.Errorf("%w: %s", ErrMissing, digest)
			}
			return 0, e
		}
		total += info.Size()
	}
	return total, nil
}

func (s *Store) cleanTemp() error {
	entries, e := os.ReadDir(s.dir)
	if e != nil {
		return e
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), ".atlas-result-") {
			if e = os.Remove(filepath.Join(s.dir, entry.Name())); e != nil {
				return e
			}
		}
	}
	return nil
}
