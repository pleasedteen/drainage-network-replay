"""Versioned sample generation and source selection.

Generated sample sets are never written straight into the target directory.
The target directory belongs to the user and is left untouched; generated
segments live in a private, versioned area below it::

    <root>/
      user-notes.txt                  kept, never deleted or overwritten
      B-0001-p0.json                  a user file that happens to share a
                                      generated shard's name -- also kept
      .samples/
        manifest.json                 {"format": 1, "current": "<version>"}
        lock                          cross-process flip/lease lock
        versions/<version>/*.json     one complete, immutable sample set
        leases/<uuid>.json            readers replaying that set right now
        tmp-*                         in-progress generations

Guarantees:

* **No user file is cleared.**  Generation only ever writes below
  ``.samples/``; even a pre-existing file named exactly like a shard survives.
* **A generation is atomic.**  Segments are written into ``tmp-*`` and the
  manifest is replaced only after every byte is on disk.  A hard kill at any
  point leaves the previously selected version complete and replayable.
* **A replay selects one set.**  ``open_source`` resolves the manifest once
  and returns that version's shard paths; other JSON files in the directory
  (user files, superseded versions) never reach the confirm/publish path,
  even if a new version is generated while the replay is running.
* **Old sets do not accumulate.**  After a successful flip, versions that are
  neither current nor leased by a live replay are deleted.  A lease keeps an
  in-flight replay's files readable; stale leases from killed processes are
  reaped.

A directory without ``.samples/manifest.json`` is a *bare* source: it keeps
the original behaviour (every ``*.json`` in it is delivered), so directories
not managed by the generator still work with ``sync --from-source``.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import socket
import tempfile
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

try:                                    # POSIX; the service ships for Linux
    import fcntl
except ImportError:                     # pragma: no cover - non-POSIX fallback
    fcntl = None

MANAGED_DIR = ".samples"
MANIFEST = "manifest.json"
VERSIONS = "versions"
LEASES = "leases"
STAGING_PREFIX = "tmp-"
FORMAT = 1
FOREIGN_LEASE_TTL = 24 * 60 * 60      # seconds a lease from another host counts


class SampleError(RuntimeError):
    """The managed sample area is damaged or internally inconsistent."""


# ---------------------------------------------------------------------------
# small filesystem helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:             # exists but owned by another user
        return True
    return True


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:                     # some filesystems refuse dir fsync
        pass
    finally:
        os.close(fd)


def _fsync_tree(root: Path) -> None:
    dirs = [root]
    for p in sorted(root.rglob("*")):
        if p.is_file():
            try:
                with open(p, "rb") as fp:
                    os.fsync(fp.fileno())
            except OSError:
                pass
        elif p.is_dir():
            dirs.append(p)
    for d in reversed(dirs):
        _fsync_dir(d)


def _atomic_write_json(path: Path, payload: dict) -> None:
    """Write ``payload`` so readers see either the old or the new file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.",
                               suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, separators=(",", ":"))
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(tmp, path)
        _fsync_dir(path.parent)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _tree_digest(root: Path) -> str:
    """Content address of a complete sample set (names + bytes)."""
    h = hashlib.sha256(b"sample-set-v1\0")
    files = [p for p in root.rglob("*") if p.is_file()]
    for p in sorted(files, key=lambda q: q.relative_to(root).as_posix()):
        rel = p.relative_to(root).as_posix().encode()
        h.update(len(rel).to_bytes(8, "big"))
        h.update(rel)
        with open(p, "rb") as fp:
            for chunk in iter(lambda: fp.read(1 << 20), b""):
                h.update(chunk)
        h.update(b"\0")
    return h.hexdigest()[:16]


# ---------------------------------------------------------------------------
# reader handle
# ---------------------------------------------------------------------------

class SampleSet:
    """The shard files one replay has selected.

    For a managed directory the selected version is pinned by a lease until
    :meth:`close`, so concurrent generation cannot delete files underneath
    the replay.  For a bare directory ``version`` is ``None`` and no lease is
    taken; ``paths`` is exactly what the legacy ``*.json`` glob returned.
    """

    def __init__(self, root: Path, version: str | None, paths: list[Path],
                 lease: Path | None = None):
        self.root = root
        self.version = version
        self.paths = list(paths)
        self._lease = lease

    @property
    def managed(self) -> bool:
        return self.version is not None

    def current_paths(self) -> list[Path]:
        """Paths to scan now.

        A managed version is immutable, so a long-running ``sync --watch``
        keeps consuming the set it selected when it started.  A bare source
        keeps the original behaviour of re-globbing so files dropped in later
        are still picked up.
        """
        if self.managed:
            return list(self.paths)
        return sorted(self.root.glob("*.json"))

    def close(self) -> None:
        if self._lease is not None:
            try:
                self._lease.unlink()
            except FileNotFoundError:
                pass
            self._lease = None

    def __enter__(self) -> "SampleSet":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


# ---------------------------------------------------------------------------
# store
# ---------------------------------------------------------------------------

class SampleStore:
    def __init__(self, root):
        self.root = Path(root)
        self.base = self.root / MANAGED_DIR
        self.manifest_path = self.base / MANIFEST
        self.versions_dir = self.base / VERSIONS
        self.leases_dir = self.base / LEASES
        self.lock_path = self.base / "lock"

    # -- layout -----------------------------------------------------------
    @property
    def managed(self) -> bool:
        return self.manifest_path.is_file()

    def version_dir(self, version: str) -> Path:
        return self.versions_dir / version

    def segment_paths(self, version: str) -> list[Path]:
        vdir = self.version_dir(version)
        if not vdir.is_dir():
            raise SampleError(f"sample version {version!r} is missing from "
                              f"{self.versions_dir}")
        return sorted(vdir.glob("*.json"))

    # -- locking ----------------------------------------------------------
    @contextmanager
    def _lock(self):
        self.base.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            if fcntl is not None:
                fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    # -- manifest ---------------------------------------------------------
    def read_manifest(self) -> dict | None:
        try:
            text = self.manifest_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        try:
            data = json.loads(text)
        except ValueError as exc:
            raise SampleError(f"corrupt sample manifest "
                              f"{self.manifest_path}: {exc}") from exc
        if not isinstance(data, dict) or data.get("format") != FORMAT:
            raise SampleError(f"unsupported sample manifest in "
                              f"{self.manifest_path}")
        current = data.get("current")
        if not isinstance(current, str) or not current:
            raise SampleError(f"sample manifest has no current version: "
                              f"{self.manifest_path}")
        return data

    # -- reading ----------------------------------------------------------
    def open(self) -> SampleSet:
        """Select the current version and lease it for the reader's lifetime."""
        if not self.managed:
            return SampleSet(self.root, None,
                             sorted(self.root.glob("*.json")))
        with self._lock():
            manifest = self.read_manifest()
            if manifest is None:        # flipped away between probes
                return SampleSet(self.root, None,
                                 sorted(self.root.glob("*.json")))
            version = manifest["current"]
            paths = self.segment_paths(version)
            lease = self._write_lease(version)
        return SampleSet(self.root, version, paths, lease)

    def _write_lease(self, version: str) -> Path:
        self.leases_dir.mkdir(parents=True, exist_ok=True)
        path = self.leases_dir / f"{uuid.uuid4().hex}.json"
        _atomic_write_json(path, {
            "version": version,
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "created": time.time(),
        })
        return path

    # -- generation -------------------------------------------------------
    def publish(self, populate) -> str:
        """Atomically install a newly generated set and return its version.

        ``populate(staging_dir)`` writes the complete set into the directory
        it is given.  It may raise at any point; the previously selected
        version stays intact and replayable.
        """
        self.base.mkdir(parents=True, exist_ok=True)
        self.versions_dir.mkdir(parents=True, exist_ok=True)
        self.leases_dir.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(dir=self.base,
                                        prefix=f"{STAGING_PREFIX}{os.getpid()}-"))
        installed = False
        try:
            populate(staging)
            _fsync_tree(staging)
            version = _tree_digest(staging)
            with self._lock():
                self._reap_staging()
                target = self.version_dir(version)
                if target.is_dir():
                    # byte-identical set already on disk: reuse it and leave
                    # the manifest untouched, so repeated runs are a no-op
                    shutil.rmtree(staging, ignore_errors=True)
                    current = self.read_manifest()
                    if current is not None and current["current"] == version:
                        return version
                else:
                    os.replace(staging, target)
                    installed = True
                    _fsync_dir(self.versions_dir)
                _atomic_write_json(self.manifest_path, {
                    "format": FORMAT,
                    "current": version,
                    "updated_at": _now(),
                })
                self._reap_versions(keep=version)
            return version
        except BaseException:
            if not installed:
                shutil.rmtree(staging, ignore_errors=True)
            raise

    # -- cleanup ----------------------------------------------------------
    def _reap_staging(self) -> None:
        """Remove staging directories left behind by dead generators."""
        for entry in self.base.iterdir():
            if not entry.is_dir() or not entry.name.startswith(STAGING_PREFIX):
                continue
            rest = entry.name[len(STAGING_PREFIX):]
            pid_txt = rest.split("-", 1)[0]
            try:
                pid = int(pid_txt)
            except ValueError:
                pid = -1
            if not _pid_alive(pid):
                shutil.rmtree(entry, ignore_errors=True)

    def _lease_protects(self, payload, path: Path) -> tuple[bool, str | None]:
        """Return ``(alive, version)`` for a parsed lease file."""
        if not isinstance(payload, dict):
            return False, None
        version = payload.get("version")
        if not isinstance(version, str):
            version = None
        host = payload.get("host")
        if host not in (None, socket.gethostname()):
            # Another machine (e.g. a container whose hostname changed after a
            # restart): liveness cannot be judged, so honour the lease only
            # while it is fresh and let genuinely abandoned sets be reclaimed.
            try:
                age = time.time() - path.stat().st_mtime
            except OSError:
                return False, version
            return age < FOREIGN_LEASE_TTL, version
        try:
            pid = int(payload.get("pid", -1))
        except (TypeError, ValueError):
            return False, version
        return _pid_alive(pid), version

    def _reap_versions(self, keep: str) -> None:
        """Delete superseded, unused versions and stale leases under lock."""
        protected = {keep}
        for lease in self.leases_dir.iterdir():
            if not lease.is_file() or lease.name.startswith("."):
                continue
            try:
                payload = json.loads(lease.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                payload = None
            alive, version = self._lease_protects(payload, lease)
            if alive and version is not None:
                protected.add(version)
                continue
            try:
                lease.unlink()
            except OSError:
                pass

        for entry in self.versions_dir.iterdir():
            if not entry.is_dir() or entry.name in protected:
                continue
            shutil.rmtree(entry, ignore_errors=True)


# ---------------------------------------------------------------------------
# entry points used by replay / sync
# ---------------------------------------------------------------------------

def open_source(root) -> SampleSet:
    """Resolve ``root`` to the shard files one replay may consume."""
    return SampleStore(root).open()


def publish_samples(root, populate) -> str:
    return SampleStore(root).publish(populate)
