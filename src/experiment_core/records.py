"""Local durable records. Records describe effects; they do not authorize them."""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile
import threading

from experiment_core.errors import Conflict


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def safe_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,79}", value):
        raise ValueError("Invalid ID")
    return value


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _sync_directory(path):
    if os.name == "posix":
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def atomic_json(path, value, *, mode=0o660):
    """Publish finite JSON with unique temporary names and durable replacement."""
    path = Path(path)
    data = canonical_json(value).encode("utf-8")
    descriptor, temporary = tempfile.mkstemp(prefix="." + path.name + ".", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            os.fchmod(stream.fileno(), mode)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _sync_directory(path.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


def read_json(path, default=None):
    try:
        with Path(path).open("rb") as stream:
            raw = stream.read(16 * 1024 * 1024 + 1)
        if len(raw) > 16 * 1024 * 1024:
            return default
        return json.loads(raw)
    except (OSError, ValueError):
        return default


class Journal:
    """SQLite intent/event store with CAS and indexed status; one state root.

    A running intent survives process death and must be reconciled, never replayed.
    Database transactions deliberately never wrap physical I/O.
    """

    def __init__(self, path):
        self._mutex = threading.RLock()
        self._temporary_locks = tempfile.TemporaryDirectory(prefix="experiment-core-") if str(path) == ":memory:" else None
        self._lock_root = (Path(self._temporary_locks.name) if self._temporary_locks is not None
                           else Path(str(Path(path).resolve()) + ".locks"))
        self._lock_root.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS requests (
                id TEXT PRIMARY KEY, scope TEXT NOT NULL, fingerprint TEXT NOT NULL,
                request TEXT NOT NULL, state TEXT NOT NULL, sequence INTEGER NOT NULL,
                result TEXT);
            CREATE INDEX IF NOT EXISTS requests_scope_state ON requests(scope, state, id);
            CREATE TABLE IF NOT EXISTS events (
                request_id TEXT NOT NULL, sequence INTEGER NOT NULL, body TEXT NOT NULL,
                PRIMARY KEY(request_id, sequence));
        """)

    @contextmanager
    def guard(self, namespace, identity):
        """Fence one bounded host action, including across journal connections.

        POSIX advisory locks are released on process death. A durable intent is
        still required before I/O: acquiring this lock never permits replay.
        The guard is deliberately non-reentrant and does not hold a DB transaction.
        All participating local adapters must use this same state root.
        """
        import fcntl
        path = self._lock_root / digest([namespace, identity])
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise Conflict("Another action is using this owner or configuration") from error
            try:
                yield
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    @contextmanager
    def transaction(self):
        with self._mutex:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield self._db
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def close(self):
        with self._mutex:
            self._db.close()
            if self._temporary_locks is not None:
                self._temporary_locks.cleanup()

    def admit(self, request_id, scope, request):
        safe_id(request_id)
        safe_id(scope)
        fingerprint = digest(request)
        with self.transaction() as db:
            old = db.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
            if old:
                if old["scope"] != scope or old["fingerprint"] != fingerprint:
                    raise Conflict("Request ID already binds different content or scope")
                return False
            db.execute("INSERT INTO requests VALUES (?,?,?,?,?,0,NULL)",
                       (request_id, scope, fingerprint, canonical_json(request), "queued"))
        return True

    def get(self, request_id):
        with self._mutex:
            row = self._db.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
        if row is None:
            raise KeyError(request_id)
        record = dict(row)
        record["request"] = json.loads(record["request"])
        record["result"] = json.loads(record["result"]) if record["result"] else None
        return record

    def transition(self, request_id, expected_sequence, state, event, result=None):
        event_json = canonical_json(event)
        result_json = canonical_json(result) if result is not None else None
        with self.transaction() as db:
            changed = db.execute(
                "UPDATE requests SET state=?, sequence=sequence+1, result=? WHERE id=? AND sequence=?",
                (state, result_json, request_id, expected_sequence)).rowcount
            if changed != 1:
                raise Conflict("Request sequence changed")
            db.execute("INSERT INTO events VALUES (?,?,?)",
                       (request_id, expected_sequence + 1, event_json))
        return expected_sequence + 1

    def events(self, request_id, *, after=0, limit=100):
        self._limit(limit)
        with self._mutex:
            rows = self._db.execute(
                "SELECT sequence,body FROM events WHERE request_id=? AND sequence>? ORDER BY sequence LIMIT ?",
                (request_id, after, limit)).fetchall()
        return [{"sequence": row["sequence"], "event": json.loads(row["body"])} for row in rows]

    def list(self, scope, *, state=None, after="", limit=100):
        self._limit(limit)
        query = "SELECT id FROM requests WHERE scope=? AND id>?"
        args = [scope, after]
        if state is not None:
            query += " AND state=?"
            args.append(state)
        with self._mutex:
            rows = self._db.execute(query + " ORDER BY id LIMIT ?", [*args, limit]).fetchall()
            return [self.get(row["id"]) for row in rows]

    @staticmethod
    def _limit(limit):
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("Page size must be between 1 and 1000")


class FileArtifacts:
    """Immutable scoped content; publish before committing a journal reference.

    Interrupted/unreferenced publications remain available for reconciliation.
    The root must be controlled by the host, not writable by untrusted callers.
    """

    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def put(self, scope, content):
        scope = safe_id(scope)
        if not isinstance(content, bytes):
            raise TypeError("Artifacts must be bytes")
        artifact_id = hashlib.sha256(content).hexdigest()
        directory = self.root / scope
        directory.mkdir(exist_ok=True)
        _sync_directory(self.root)
        destination = directory / artifact_id
        descriptor, temporary = tempfile.mkstemp(prefix=".staged-", dir=directory)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, destination)
                _sync_directory(directory)
            except FileExistsError:
                if destination.read_bytes() != content:
                    raise Conflict("Artifact digest collision or damaged artifact")
        finally:
            Path(temporary).unlink(missing_ok=True)
        return {"scope": scope, "artifact_id": artifact_id, "size": len(content)}

    def read(self, scope, artifact_id):
        safe_id(scope)
        if not isinstance(artifact_id, str) or not re.fullmatch(r"[0-9a-f]{64}", artifact_id):
            raise ValueError("Invalid artifact digest")
        content = (self.root / scope / artifact_id).read_bytes()
        if hashlib.sha256(content).hexdigest() != artifact_id:
            raise Conflict("Artifact integrity check failed")
        return content
