#!/usr/bin/env python3 -I
"""Empirical evidence for migrating render PNGs from inline state to an
independent content-addressed result store.

The script drives two real HTTP server binaries:
  * a legacy binary (state format v1, PNG embedded in the JSON state)
  * the new binary (state format v2, PNG in a separate result store)

It proves, with byte-level checks:
  1. Migration: purged and live results both migrate; identical outputs share
    one blob; downloads, compare, purge and history queries stay compatible.
    A crash between blob write and the state rewrite resumes idempotently.
  2. Restart recovery: running jobs recover after SIGKILL; a crash between
    blob write and state commit cannot publish a phantom result and leaves an
    orphan blob that restart reconciles; tampering is detected.
  3. Concurrent cleanup: purging one job never deletes bytes another job is
    still serving.

All paths are passed as arguments; no imports outside the standard library.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import zlib


def sample_doc():
    return {"width": 320, "height": 240, "background": "#F5F1E8", "layers": [
        {"id": "plate", "kind": "rectangle", "width": 220, "height": 150,
         "transform": {"x": 45, "y": 35, "scaleX": 1, "scaleY": 1, "degrees": 5},
         "opacity": 1, "fill": {"kind": "linear", "color": "#145A70",
                                "endColor": "#32A5A0", "from": {"x": 0, "y": 0},
                                "to": {"x": 220, "y": 150}}},
        {"id": "accent", "kind": "ellipse", "width": 130, "height": 130,
         "transform": {"x": 150, "y": 95, "scaleX": 1, "scaleY": 1, "degrees": 0},
         "opacity": .85, "fill": {"kind": "solid", "color": "#E87E4F"}}]}


def heavy_doc():
    doc = sample_doc()
    doc["width"] = 1500
    doc["height"] = 1500
    doc["layers"] = [
        dict(doc["layers"][0], id="sheet-%d" % i, width=1500, height=1500,
             transform={"scaleX": 1, "scaleY": 1}) for i in range(16)]
    return doc


def sha(data):
    return hashlib.sha256(data).hexdigest()


class Server:
    def __init__(self, binary, directory, name):
        self.binary = binary
        self.directory = Path(directory)
        self.name = name
        self.proc = None
        self.log_path = self.directory / (name + ".log")
        self.log = open(self.log_path, "ab")
        self.start()

    def start(self):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        self.url = "http://127.0.0.1:%d" % port
        env = dict(os.environ, ATLAS_ADDR="127.0.0.1:%d" % port,
                   ATLAS_STATE=str(self.directory / "state.json"))
        self.proc = subprocess.Popen([str(self.binary)], env=env,
                                     stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                self.call("GET", "/healthz")
                return
            except (urllib.error.URLError, ConnectionError):
                if self.proc.poll() is not None:
                    raise RuntimeError(self.log_path.read_text())
                time.sleep(0.03)
        raise RuntimeError("startup deadline exceeded")

    def stop(self, abrupt=False):
        if self.proc and self.proc.poll() is None:
            (self.proc.kill if abrupt else self.proc.terminate)()
            self.proc.wait(timeout=15)

    def call(self, method, path, body=None, status=200, headers=None):
        hdr = dict(headers or {})
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
            hdr["Content-Type"] = "application/json"
        req = urllib.request.Request(self.url + path, data=body,
                                     method=method, headers=hdr)
        try:
            resp = urllib.request.urlopen(req, timeout=20)
        except urllib.error.HTTPError as err:
            resp = err
        with resp:
            data = resp.read()
            if resp.status != status:
                raise AssertionError("%s %s: expected %d got %d: %r"
                                     % (method, path, status, resp.status, data[:300]))
            if resp.headers.get("Content-Type", "").startswith("application/json"):
                return json.loads(data)
            return data

    def wait(self, job_id, wanted=("succeeded",)):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            job = self.call("GET", "/v1/jobs/" + job_id)
            if job["state"] in wanted:
                return job
            if job["state"] in ("failed", "cancelled"):
                raise RuntimeError("unexpected outcome: %s" % job)
            time.sleep(0.025)
        raise RuntimeError("job deadline exceeded")

    def submit(self, snapshot, key, scale=1):
        return self.call("POST", "/v1/jobs",
                         {"snapshotId": snapshot["id"], "scale": scale}, 202,
                         {"Idempotency-Key": key})

    def setup_scene(self, doc, rev=0, scene_id="emblem"):
        return self.call("PUT", "/v1/scenes/" + scene_id,
                         {"name": "证据场景", "expectedRevision": rev, "document": doc})

    def freeze(self, scene_id, rev):
        return self.call("POST", "/v1/scenes/%s/snapshots" % scene_id,
                         {"expectedRevision": rev}, 201)


def png_header_matches(data, width, height):
    if data[:8] != b"\x89PNG\r\n\x1a\n":
        return False
    return struct.unpack(">II", data[16:24]) == (width, height)


def blob_files(results_dir):
    return sorted(p.name for p in Path(results_dir).iterdir()
                  if len(p.name) == 64 and all(c in "0123456789abcdef" for c in p.name))


def temp_files(results_dir):
    return [p.name for p in Path(results_dir).iterdir()
            if p.name.startswith(".atlas-result-") or p.name.startswith(".atlas-write-")]


def phase_migration(root, legacy_bin, new_bin, report):
    work = root / "migration"
    legacy_dir = work / "legacy-state"
    legacy_dir.mkdir(parents=True)

    # ---- Produce a genuine format-v1 state with the legacy binary ----
    legacy = Server(legacy_bin, legacy_dir, "legacy")
    try:
        doc = sample_doc()
        legacy.setup_scene(doc)
        scene = legacy.call("GET", "/v1/scenes/emblem")
        snap = legacy.freeze("emblem", scene["revision"])
        # Two jobs over the same fixed input produce byte-identical PNGs.
        job1 = legacy.wait(legacy.submit(snap, "key-one")["id"])
        job2 = legacy.wait(legacy.submit(snap, "key-two")["id"])
        png1 = legacy.call("GET", "/v1/artifacts/" + job1["artifactId"])
        png2 = legacy.call("GET", "/v1/artifacts/" + job2["artifactId"])
        assert png1 == png2, "identical inputs must yield identical bytes"
        # A second, visibly different render to have a distinct digest.
        doc2 = sample_doc()
        doc2["background"] = "#102030"
        legacy.setup_scene(doc2, rev=1)
        scene2 = legacy.call("GET", "/v1/scenes/emblem")
        snap2 = legacy.freeze("emblem", scene2["revision"])
        job3 = legacy.wait(legacy.submit(snap2, "key-three")["id"])
        png3 = legacy.call("GET", "/v1/artifacts/" + job3["artifactId"])
        assert png3 != png1
        # Purge one result while still on the legacy format.
        legacy.call("DELETE", "/v1/artifacts/" + job3["artifactId"])
        purged_gone = legacy.call("GET", "/v1/artifacts/" + job3["artifactId"], status=410)
        report["legacy"]["purgedAnswers410"] = bool(purged_gone is not None)
        ids = {"job1": job1["id"], "job2": job2["id"], "job3": job3["id"],
               "art1": job1["artifactId"], "art2": job2["artifactId"],
               "art3": job3["artifactId"]}
        report["legacy"]["digests"] = {"png1": sha(png1), "png2": sha(png2), "png3": sha(png3)}
        report["legacy"]["bytes"] = {"png1": len(png1), "png2": len(png2), "png3": len(png3)}
    finally:
        legacy.stop()

    state_file = legacy_dir / "state.json"
    raw_before = state_file.read_bytes()
    legacy_state = json.loads(raw_before)
    assert legacy_state["version"] == 1, "fixture must be format v1"
    inline_png_fields = [a for a in legacy_state["artifacts"].values() if "png" in a]
    report["legacy"]["stateVersion"] = 1
    report["legacy"]["artifactsWithInlinePngField"] = len(inline_png_fields)
    # Inline base64 payload must actually be present in the legacy state file.
    assert any("png" in a for a in legacy_state["artifacts"].values())
    size_before = len(raw_before)

    # Snapshot the legacy directory so the original evidence stays intact.
    new_dir = work / "migrated-state"
    shutil.copytree(legacy_dir, new_dir)
    results_dir = new_dir / "state.json.results"
    assert not results_dir.exists(), "no result store may exist before migration"

    # ---- Open the same state with the new binary: migration runs on startup ----
    new = Server(new_bin, new_dir, "new")
    try:
        migrated_state = json.loads((new_dir / "state.json").read_text())
        report["migration"]["stateVersionAfterOpen"] = migrated_state["version"]
        assert migrated_state["version"] == 2
        assert all("png" not in a for a in migrated_state["artifacts"].values()), \
            "no artifact may embed png after migration"
        report["migration"]["artifactsWithInlinePngField"] = 0
        report["migration"]["stateJsonBytesBefore"] = size_before
        report["migration"]["stateJsonBytesAfter"] = len((new_dir / "state.json").read_bytes())

        blobs = blob_files(results_dir)
        report["migration"]["resultBlobCount"] = len(blobs)
        # Two live identical PNGs => one shared blob; purged => no blob.
        assert len(blobs) == 1, "expected one shared live blob, got %r" % blobs
        shared_digest = blobs[0]
        assert shared_digest == report["legacy"]["digests"]["png1"] == report["legacy"]["digests"]["png2"]
        blob_bytes = (results_dir / shared_digest).read_bytes()
        report["migration"]["blobByteDigest"] = sha(blob_bytes)
        assert sha(blob_bytes) == report["legacy"]["digests"]["png1"]
        report["migration"]["blobByteLength"] = len(blob_bytes)
        assert len(blob_bytes) == report["legacy"]["bytes"]["png1"]

        # Download compatibility: bytes are identical to the legacy payload.
        got1 = new.call("GET", "/v1/artifacts/" + ids["art1"])
        got2 = new.call("GET", "/v1/artifacts/" + ids["art2"])
        assert got1 == png1 and got2 == png2, "downloaded bytes must match legacy bytes"
        report["migration"]["downloadBytesMatchLegacy"] = True
        etag = None
        req = urllib.request.Request(new.url + "/v1/artifacts/" + ids["art1"], method="GET")
        with urllib.request.urlopen(req, timeout=10) as resp:
            etag = resp.headers.get("ETag")
            resp.read()
        report["migration"]["etag"] = etag
        assert etag == '"%s"' % shared_digest
        # Conditional request still answers 304.
        got304 = new.call("GET", "/v1/artifacts/" + ids["art1"],
                          headers={"If-None-Match": etag}, status=304)
        report["migration"]["conditional304"] = (got304 == b"")

        # Already-purged legacy result stays 410 and its job stays succeeded.
        new.call("GET", "/v1/artifacts/" + ids["art3"], status=410)
        job3_after = new.call("GET", "/v1/jobs/" + ids["job3"])
        assert job3_after["state"] == "succeeded" and job3_after["artifactId"] == ids["art3"]
        report["migration"]["purgedStaysPurgedJobSucceeded"] = True

        # History query compatibility for all three jobs.
        for key in ("job1", "job2", "job3"):
            j = new.call("GET", "/v1/jobs/" + ids[key])
            assert j["state"] == "succeeded"
        report["migration"]["historyJobsAllSucceeded"] = True

        # Compare compatibility (pixel semantics, decoded from stored blobs).
        cmp_same = new.call("POST", "/v1/compare",
                            {"left": ids["art1"], "right": ids["art2"]})
        assert cmp_same["changedPixels"] == 0 and cmp_same["maxDelta"] == 0
        report["migration"]["compareIdentical"] = cmp_same
        # Comparing against the purged artifact must fail with 410.
        new.call("POST", "/v1/compare",
                 {"left": ids["art1"], "right": ids["art3"]}, status=410)
        report["migration"]["comparePurgedAnswers410"] = True

        # Status reports retained on-disk bytes (single shared blob once).
        status = new.call("GET", "/v1/status")
        report["migration"]["statusArtifactBytes"] = status["artifactBytes"]
        assert status["artifactBytes"] == len(blob_bytes)
        assert status["jobs"]["succeeded"] == 3
    finally:
        new.stop()

    # Restart on migrated state: blobs re-verified, no format regression.
    again = Server(new_bin, new_dir, "new-restart")
    try:
        st = json.loads((new_dir / "state.json").read_text())
        assert st["version"] == 2
        got = again.call("GET", "/v1/artifacts/" + ids["art1"])
        assert got == png1
        report["migration"]["restartStillServesSameBytes"] = True
        # Purge one of the two sharing jobs: shared bytes must survive.
        again.call("DELETE", "/v1/artifacts/" + ids["art1"])
        again.call("GET", "/v1/artifacts/" + ids["art1"], status=410)
        surviving = again.call("GET", "/v1/artifacts/" + ids["art2"])
        assert surviving == png2
        assert blob_files(results_dir) == [shared_digest], \
            "purge of one job must not delete another job's shared bytes"
        report["migration"]["purgeOneKeepsSharedBlob"] = True
        # Purge the last user: the blob is released.
        again.call("DELETE", "/v1/artifacts/" + ids["art2"])
        remaining = blob_files(results_dir)
        assert remaining == [], "unreferenced blob should be swept, got %r" % remaining
        report["migration"]["purgeLastReleasesBlob"] = True
        # Jobs remain historical facts after both purges.
        for key in ("job1", "job2"):
            j = again.call("GET", "/v1/jobs/" + ids[key])
            assert j["state"] == "succeeded"
        report["migration"]["jobsRemainAfterPurge"] = True
    finally:
        again.stop()
    return ids


def phase_mid_migration_crash(root, legacy_bin, new_bin, report):
    work = root / "migration-crash"
    legacy_dir = work / "legacy"
    legacy_dir.mkdir(parents=True)
    legacy = Server(legacy_bin, legacy_dir, "legacy")
    try:
        legacy.setup_scene(sample_doc())
        scene = legacy.call("GET", "/v1/scenes/emblem")
        snap = legacy.freeze("emblem", scene["revision"])
        job = legacy.wait(legacy.submit(snap, "crash-key")["id"])
        png = legacy.call("GET", "/v1/artifacts/" + job["artifactId"])
        expected_digest = sha(png)
    finally:
        legacy.stop()

    partial_dir = work / "partial"
    shutil.copytree(legacy_dir, partial_dir)
    state_path = partial_dir / "state.json"
    results_dir = partial_dir / "state.json.results"
    # Reconstruct the exact on-disk state of a crash between blob durability
    # and the v2 state rewrite: v1 JSON intact, one blob already present.
    results_dir.mkdir()
    (results_dir / expected_digest).write_bytes(png)
    v1 = json.loads(state_path.read_text())
    assert v1["version"] == 1 and "png" in next(iter(v1["artifacts"].values()))

    restarted = Server(new_bin, partial_dir, "resumed")
    try:
        v2 = json.loads(state_path.read_text())
        report["migrationCrash"]["stateVersionAfterResume"] = v2["version"]
        assert v2["version"] == 2
        assert all("png" not in a for a in v2["artifacts"].values())
        got = restarted.call("GET", "/v1/artifacts/" + job["artifactId"])
        assert got == png, "resumed migration must serve identical bytes"
        assert sha(got) == expected_digest
        report["migrationCrash"]["bytesIdenticalAfterResume"] = True
        blobs = blob_files(results_dir)
        assert blobs == [expected_digest], "pre-existing blob must be reused, not duplicated"
        report["migrationCrash"]["blobReusedNotDuplicated"] = True
        # A second startup is a normal v2 open with no migration side effects.
        restarted.stop()
        second = Server(new_bin, partial_dir, "second-open")
        try:
            assert json.loads(state_path.read_text())["version"] == 2
            assert second.call("GET", "/v1/artifacts/" + job["artifactId"]) == png
            report["migrationCrash"]["secondOpenStable"] = True
        finally:
            second.stop()
    finally:
        if restarted.proc and restarted.proc.poll() is None:
            restarted.stop()


def phase_recovery(root, new_bin, report):
    work = root / "recovery"
    d = work / "live"
    d.mkdir(parents=True)
    srv = Server(new_bin, d, "recovery")
    results_dir = d / "state.json.results"
    try:
        srv.setup_scene(heavy_doc())
        scene = srv.call("GET", "/v1/scenes/emblem")
        snap = srv.freeze("emblem", scene["revision"])
        job = srv.submit(snap, "heavy-key")
        srv.wait(job["id"], wanted=("running",))
        # SIGKILL mid-render: no state commit can describe a finished result.
        srv.stop(abrupt=True)
        state_while_down = json.loads((d / "state.json").read_text())
        down_job = state_while_down["jobs"][job["id"]]
        assert down_job["state"] == "running"
        assert down_job.get("artifactId", "") == ""
        report["recovery"]["killedWhileRunningState"] = down_job["state"]
        assert job["id"] not in state_while_down.get("artifacts", {})
        assert blob_files(results_dir) == [], "no blob may exist before success"
    finally:
        if srv.proc and srv.proc.poll() is None:
            srv.stop(abrupt=True)

    srv2 = Server(new_bin, d, "recovery-restart")
    try:
        recovered = srv2.wait(job["id"])
        assert recovered["state"] == "succeeded" and recovered["attempt"] == 1
        report["recovery"]["recoveredJobState"] = recovered["state"]
        report["recovery"]["attemptUnchanged"] = recovered["attempt"]
        png = srv2.call("GET", "/v1/artifacts/" + recovered["artifactId"])
        assert png_header_matches(png, 1500, 1500)
        report["recovery"]["pngDigestAfterRecovery"] = sha(png)
        report["recovery"]["pngBytesAfterRecovery"] = len(png)
        blobs = blob_files(results_dir)
        assert len(blobs) == 1 and blobs[0] == sha(png)
        # Digest recorded in state equals the on-disk blob.
        st = json.loads((d / "state.json").read_text())
        assert st["artifacts"][recovered["artifactId"]]["digest"] == sha(png)
        report["recovery"]["stateDigestEqualsBlob"] = True
        artifact_id = recovered["artifactId"]
    finally:
        srv2.stop()

    # Orphan reconciliation: inject a blob that no artifact names (the residue
    # of a crash between durable Put and the state transaction).
    orphan = b"\x89PNG\r\n\x1a\n orphan " + os.urandom(32)
    (results_dir / sha(orphan)).write_bytes(orphan)
    # And an abandoned staging file from an interrupted Put.
    (results_dir / ".atlas-result-stale").write_bytes(b"partial write")
    before = set(blob_files(results_dir))
    assert sha(orphan) in before
    srv3 = Server(new_bin, d, "recovery-sweep")
    try:
        after = set(blob_files(results_dir))
        report["recovery"]["orphanReconciledOnStartup"] = sha(orphan) not in after
        assert sha(orphan) not in after, "orphan blob must be swept at startup"
        assert temp_files(results_dir) == [], "staging file must be cleaned"
        # The legitimate result is untouched and still serves identical bytes.
        st = json.loads((d / "state.json").read_text())
        good = st["artifacts"][artifact_id]["digest"]
        assert good in after
        again_png = srv3.call("GET", "/v1/artifacts/" + artifact_id)
        assert sha(again_png) == report["recovery"]["pngDigestAfterRecovery"]
        report["recovery"]["legitimateBlobUnchanged"] = True
    finally:
        srv3.stop()

    # Tamper detection: corrupting a backing blob must fail startup instead of
    # serving wrong bytes for an existing state record.
    st = json.loads((d / "state.json").read_text())
    good = st["artifacts"][artifact_id]["digest"]
    target = results_dir / good
    original = target.read_bytes()
    target.write_bytes(original[:-1] + bytes([original[-1] ^ 0xFF]))
    broken = Server.__new__(Server)  # no start; do it manually to capture failure
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = dict(os.environ, ATLAS_ADDR="127.0.0.1:%d" % port,
               ATLAS_STATE=str(d / "state.json"))
    proc = subprocess.Popen([str(new_bin)], env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        rc = proc.wait(timeout=15)
        report["recovery"]["tamperedBlobStartupExitCode"] = rc
        assert rc != 0, "server must refuse to start with a corrupt live blob"
    finally:
        if proc.poll() is None:
            proc.kill()
    target.write_bytes(original)
    fixed = Server(new_bin, d, "recovery-fixed")
    try:
        ok_png = fixed.call("GET", "/v1/artifacts/" + artifact_id)
        assert sha(ok_png) == report["recovery"]["pngDigestAfterRecovery"]
        report["recovery"]["servesAgainAfterRestore"] = True
    finally:
        fixed.stop()


def phase_concurrent_purge(root, new_bin, report):
    d = root / "concurrent"
    d.mkdir(parents=True)
    srv = Server(new_bin, d, "concurrent")
    results_dir = d / "state.json.results"
    try:
        srv.setup_scene(sample_doc())
        scene = srv.call("GET", "/v1/scenes/emblem")
        snap = srv.freeze("emblem", scene["revision"])
        # Four identical jobs share one content-addressed blob.
        arts = []
        expected = None
        for i in range(4):
            job = srv.wait(srv.submit(snap, "share-%d" % i)["id"])
            png = srv.call("GET", "/v1/artifacts/" + job["artifactId"])
            expected = png if expected is None else expected
            assert png == expected
            arts.append(job["artifactId"])
        assert len(blob_files(results_dir)) == 1
        digest = blob_files(results_dir)[0]

        stop_flag = threading.Event()
        errors = []

        def reader():
            while not stop_flag.is_set():
                # Always read an artifact that must remain live (the last one).
                try:
                    data = srv.call("GET", "/v1/artifacts/" + arts[-1])
                    if sha(data) != digest:
                        errors.append("digest drift on live artifact")
                except urllib.error.HTTPError as err:
                    if err.code != 410:
                        errors.append("unexpected status %d" % err.code)
                except Exception as exc:  # network-level races are failures too
                    errors.append(repr(exc))

        threads = [threading.Thread(target=reader) for _ in range(4)]
        for t in threads:
            t.start()
        # Purge the first three owners one after another; the blob must remain
        # because arts[-1] is still live and being downloaded continuously.
        for art in arts[:-1]:
            srv.call("DELETE", "/v1/artifacts/" + art)
            srv.call("GET", "/v1/artifacts/" + art, status=410)
            assert blob_files(results_dir) == [digest], \
                "cleanup deleted shared content still in use"
        time.sleep(0.5)
        stop_flag.set()
        for t in threads:
            t.join(timeout=10)
        report["concurrent"]["readerErrors"] = errors
        assert not errors, errors
        report["concurrent"]["sharedBlobSurvivedThreePurges"] = True
        surviving = srv.call("GET", "/v1/artifacts/" + arts[-1])
        assert sha(surviving) == digest
        # Final purge releases the now-unreferenced blob.
        srv.call("DELETE", "/v1/artifacts/" + arts[-1])
        assert blob_files(results_dir) == []
        report["concurrent"]["finalPurgeReleasesBlob"] = True
        # Every purged artifact answers 410 while its job fact stays queryable.
        for art in arts:
            srv.call("GET", "/v1/artifacts/" + art, status=410)
        report["concurrent"]["allArtifactsPurgedAnswers410"] = True
    finally:
        srv.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy", required=True, help="path to format-v1 binary")
    parser.add_argument("--new", required=True, help="path to format-v2 binary")
    parser.add_argument("--out", required=True, help="evidence output directory")
    args = parser.parse_args()
    root = Path(args.out)
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    report = {"legacy": {}, "migration": {}, "migrationCrash": {},
              "recovery": {}, "concurrent": {}}
    phase_migration(root, Path(args.legacy), Path(args.new), report)
    phase_mid_migration_crash(root, Path(args.legacy), Path(args.new), report)
    phase_recovery(root, Path(args.new), report)
    phase_concurrent_purge(root, Path(args.new), report)
    report["ok"] = True
    out = root / "evidence-report.json"
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print("\nEVIDENCE WRITTEN: %s" % out)


if __name__ == "__main__":
    main()
