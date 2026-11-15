package service

import (
	"atlas-render/internal/model"
	"atlas-render/internal/raster"
	"atlas-render/internal/resultstore"
	"atlas-render/internal/scene"
	"errors"
	"fmt"
	"log"
	"time"
)

func (s *Service) Submit(snapshotID string, scale int, key string) (model.Job, error) {
	var out model.Job
	if !scene.ValidID(key) {
		return out, model.Invalid("idempotency key required (1..80 identifier characters)")
	}
	fingerprint := model.CanonicalDigest([]any{snapshotID, scale})
	e := s.Store.Update(func(st *model.State) error {
		if r, ok := st.Receipts[key]; ok {
			if r.Fingerprint != fingerprint {
				return model.Conflict("idempotency key used for different request")
			}
			out = st.Jobs[r.JobID]
			return nil
		}
		snap, ok := st.Snapshots[snapshotID]
		if !ok {
			return model.Missing("snapshot not found")
		}
		if snap.State != "active" {
			return model.Conflict("snapshot is revoked")
		}
		if _, e := raster.Compile(snap, scale); e != nil {
			return e
		}
		pending := 0
		for _, j := range st.Jobs {
			if j.State == "queued" || j.State == "running" {
				pending++
			}
		}
		if pending >= 64 {
			return model.Busy("render queue is full")
		}
		now := time.Now().UTC()
		out = model.Job{ID: model.ID("job", st.Sequence+1), SnapshotID: snapshotID, State: "queued", Scale: scale, Attempt: 1, CreatedAt: now, UpdatedAt: now, Payload: snap}
		st.Jobs[out.ID] = out
		st.Receipts[key] = model.Receipt{Key: key, Fingerprint: fingerprint, JobID: out.ID}
		st.Emit("job.queued", out.ID)
		return nil
	})
	return out, e
}
func (s *Service) Cancel(id string) (model.Job, error) {
	var out model.Job
	e := s.Store.Update(func(st *model.State) error {
		j, ok := st.Jobs[id]
		if !ok {
			return model.Missing("job not found")
		}
		if j.State == "cancelled" {
			out = j
			return nil
		}
		if j.State != "queued" && j.State != "running" {
			return model.Conflict("job has already finished")
		}
		j.State = "cancelled"
		j.UpdatedAt = time.Now().UTC()
		st.Jobs[id] = j
		st.Emit("job.cancelled", id)
		out = j
		return nil
	})
	return out, e
}
func (s *Service) Retry(id string) (model.Job, error) {
	var out model.Job
	e := s.Store.Update(func(st *model.State) error {
		j, ok := st.Jobs[id]
		if !ok {
			return model.Missing("job not found")
		}
		if j.State != "failed" && j.State != "cancelled" {
			return model.Conflict("only failed or cancelled jobs can retry")
		}
		n := 0
		for _, v := range st.Jobs {
			if v.State == "queued" || v.State == "running" {
				n++
			}
		}
		if n >= 64 {
			return model.Busy("render queue is full")
		}
		j.State = "queued"
		j.Error = ""
		j.Attempt++
		j.UpdatedAt = time.Now().UTC()
		st.Jobs[id] = j
		st.Emit("job.retried", id)
		out = j
		return nil
	})
	return out, e
}

// Purge marks the result purged in the state transaction and only then
// releases result bytes that no remaining live artifact references. Because
// blobs are content addressed, bytes shared with another job stay on disk
// until that job is purged too.
func (s *Service) Purge(id string) error {
	e := s.Store.Update(func(st *model.State) error {
		a, ok := st.Artifacts[id]
		if !ok {
			return model.Missing("artifact not found")
		}
		if a.State == "purged" {
			return nil
		}
		a.State = "purged"
		a.PNG = nil
		st.Artifacts[id] = a
		st.Emit("artifact.purged", id)
		return nil
	})
	if e != nil {
		return e
	}
	// Best effort: the purge fact is already durable. A sweep failure leaves
	// an unreferenced blob on disk, which the next startup reconciles, rather
	// than failing the request or touching another task's bytes.
	if sweepErr := s.Store.ReleaseUnreferencedResults(); sweepErr != nil {
		log.Printf("purge artifact %s: release unreferenced results: %v", id, sweepErr)
	}
	return nil
}

// Artifact returns the result record together with its verified bytes for
// live results. Purged results keep their job facts but answer 410; a state
// record whose backing blob is gone or corrupt is an internal failure, never
// a silently empty download.
func (s *Service) Artifact(id string) (model.Artifact, error) {
	a, ok := s.Store.Read().Artifacts[id]
	if !ok {
		return a, model.Missing("artifact not found")
	}
	if a.State == "purged" {
		return a, model.Gone(fmt.Sprintf("artifact %s was purged", id))
	}
	data, e := s.Store.Results().Read(a.Digest)
	if e != nil {
		if errors.Is(e, resultstore.ErrMissing) {
			return a, fmt.Errorf("artifact %s backing result is unavailable: %w", id, e)
		}
		return a, e
	}
	a.PNG = data
	return a, nil
}
