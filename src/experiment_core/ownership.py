"""Reference local reservations; adapters supply physical cleanup and topology.

Only a trusted host constructs this service and hands grants to its adapters.
It is not an RPC authenticator. Expiry/restart quarantines instead of freeing.
"""
from dataclasses import dataclass, replace
import json
import secrets
import time

from experiment_core.errors import AuthorizationRequired, Conflict, OwnerFault, StaleContext
from experiment_core.records import canonical_json, safe_id


@dataclass(frozen=True)
class Grant:
    system_id: str
    owner_id: str
    generation: str
    lease_id: str
    policy_revision: str
    request_id: str
    resources: frozenset

    def child(self, request_id, resources):
        resources = frozenset(resources)
        if not resources or not resources <= self.resources:
            raise AuthorizationRequired("A child may only restrict its parent's resources")
        return replace(self, request_id=safe_id(request_id), resources=resources)


class Reservations:
    def __init__(self, journal, owner_id, *, clock=time.time, expand_resources=None):
        self.journal = journal
        self.owner_id = safe_id(owner_id)
        self.generation = secrets.token_hex(16)
        self.clock = clock
        self.expand_resources = expand_resources or frozenset
        with journal.guard("owner", self.owner_id), journal.transaction() as db:
            db.execute("CREATE TABLE IF NOT EXISTS owner_generations "
                       "(owner_id TEXT PRIMARY KEY, generation TEXT NOT NULL)")
            db.execute("INSERT INTO owner_generations VALUES (?,?) "
                       "ON CONFLICT(owner_id) DO UPDATE SET generation=excluded.generation",
                       (self.owner_id, self.generation))
            db.execute("""CREATE TABLE IF NOT EXISTS leases (
                lease_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, generation TEXT NOT NULL,
                system_id TEXT NOT NULL, revision TEXT NOT NULL, resources TEXT NOT NULL,
                expires REAL NOT NULL, state TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 0, cleanup TEXT)""")
            columns = {row["name"] for row in db.execute("PRAGMA table_info(leases)")}
            if "version" not in columns:
                db.execute("ALTER TABLE leases ADD COLUMN version INTEGER NOT NULL DEFAULT 0")
            if "cleanup" not in columns:
                db.execute("ALTER TABLE leases ADD COLUMN cleanup TEXT")
            db.execute("CREATE INDEX IF NOT EXISTS leases_owner ON leases(owner_id,state)")
            db.execute("UPDATE leases SET state='quarantined',version=version+1 "
                       "WHERE owner_id=? AND state IN ('active','reconciling')",
                       (self.owner_id,))

    def _assert_current(self, db):
        row = db.execute("SELECT generation FROM owner_generations WHERE owner_id=?",
                         (self.owner_id,)).fetchone()
        if row is None or row["generation"] != self.generation:
            raise StaleContext("Owner generation changed; this instance is retired")

    def acquire(self, system_id, request_id, resources, policy_revision, *, ttl_s):
        import json
        import math
        safe_id(system_id)
        safe_id(request_id)
        if not isinstance(policy_revision, str) or not policy_revision:
            raise ValueError("An effective policy revision is required")
        if type(ttl_s) not in (int, float) or not math.isfinite(ttl_s) or ttl_s <= 0:
            raise ValueError("A finite positive lease duration is required")
        resources = frozenset(self.expand_resources(frozenset(resources)))
        if not resources or any(not isinstance(x, str) or not x for x in resources):
            raise ValueError("Nonempty physical resource identities are required")
        with self.journal.transaction() as db:
            self._assert_current(db)
            db.execute("UPDATE leases SET state='quarantined' WHERE owner_id=? AND state='active' AND expires<=?",
                       (self.owner_id, self.clock()))
            # Physical identities are global within the shared state root.
            rows = db.execute("SELECT resources FROM leases WHERE state!='released'").fetchall()
            if any(resources.intersection(json.loads(row["resources"])) for row in rows):
                raise Conflict("Resource is reserved or awaiting physical reconciliation")
            token = secrets.token_hex(32)
            db.execute("INSERT INTO leases (lease_id,owner_id,generation,system_id,revision,resources,expires,state) "
                       "VALUES (?,?,?,?,?,?,?,?)", (
                token, self.owner_id, self.generation, system_id, policy_revision,
                canonical_json(sorted(resources)), self.clock() + ttl_s, "active"))
        return Grant(system_id, self.owner_id, self.generation, token, policy_revision, request_id, resources)

    def validate(self, grant, *, policy_revision):
        import json
        with self.journal.transaction() as db:
            self._assert_current(db)
            row = db.execute("SELECT * FROM leases WHERE lease_id=?", (grant.lease_id,)).fetchone()
            if row is None or grant.owner_id != self.owner_id or row["owner_id"] != self.owner_id:
                raise AuthorizationRequired("Unknown owner grant")
            if grant.generation != self.generation or row["generation"] != self.generation:
                raise StaleContext("Owner generation changed")
            if row["state"] != "active":
                raise OwnerFault("Reservation is not active")
            if row["expires"] <= self.clock():
                # Persist quarantine before raising outside this transaction.
                db.execute("UPDATE leases SET state='quarantined' WHERE lease_id=?", (grant.lease_id,))
                expired = True
            else:
                expired = False
            if (grant.system_id != row["system_id"] or not grant.resources or
                    not grant.resources <= frozenset(json.loads(row["resources"]))):
                raise AuthorizationRequired("Grant scope does not match reservation")
            if grant.policy_revision != row["revision"] or policy_revision != row["revision"]:
                raise StaleContext("Effective policy revision changed")
        if expired:
            raise OwnerFault("Lease expired; physical reconciliation is required")

    def quarantine(self, grant):
        # A retired instance may still report its own uncertain lease, but may
        # never alter a replacement instance's grant or release anything.
        if grant.owner_id != self.owner_id:
            raise AuthorizationRequired("Unknown owner grant")
        if grant.generation != self.generation:
            raise StaleContext("Grant belongs to a different owner generation")
        with self.journal.transaction() as db:
            db.execute("UPDATE leases SET state='quarantined',version=version+1 "
                       "WHERE lease_id=? AND owner_id=? AND generation=? AND state!='released'",
                       (grant.lease_id, self.owner_id, grant.generation))

    def validate_cleanup(self, grant):
        """Validate cancellation authority without stale policy/time admission."""
        with self.journal.transaction() as db:
            self._assert_current(db)
            row = db.execute("SELECT * FROM leases WHERE lease_id=? AND owner_id=?",
                             (grant.lease_id, self.owner_id)).fetchone()
            if row is None or grant.owner_id != self.owner_id:
                raise AuthorizationRequired("Unknown owner grant")
            if grant.generation != self.generation or row["generation"] != self.generation:
                raise StaleContext("Owner generation changed; use owner reconciliation")
            if (grant.system_id != row["system_id"] or not grant.resources or
                    not grant.resources <= frozenset(json.loads(row["resources"]))):
                raise AuthorizationRequired("Cleanup scope does not match reservation")
            if row["state"] not in {"active", "quarantined"}:
                raise OwnerFault("Reservation is already released or reconciling")

    def reconcile(self, lease_id, quiesce_and_inspect):
        """Fence admission, then ask a trusted owner to drain and inspect work.

        The callback receives the durable lease/fence snapshot. It must stop or
        drain native/background writers, reconcile any previous interrupted
        cleanup without blind replay, and return fresh reusable-state evidence.
        Executor steps share the owner guard; non-Executor writers must be fenced
        by this application callback. An observation of a momentarily safe value
        alone cannot release a lease.
        """
        with self.journal.guard("owner", self.owner_id):
            with self.journal.transaction() as db:
                self._assert_current(db)
                row = db.execute("SELECT * FROM leases WHERE lease_id=? AND owner_id=?",
                                 (lease_id, self.owner_id)).fetchone()
                if row is None:
                    raise AuthorizationRequired("Unknown reservation")
                if row["state"] == "released":
                    if row["cleanup"] is None:
                        raise OwnerFault("Legacy released reservation has no retained cleanup evidence")
                    return json.loads(row["cleanup"])
                snapshot = dict(row)
                snapshot["resources"] = json.loads(snapshot["resources"])
                version = row["version"] + 1
                snapshot["fence_version"] = version
                db.execute("UPDATE leases SET state='reconciling',version=? WHERE lease_id=?",
                           (version, lease_id))
            try:
                evidence = quiesce_and_inspect(snapshot)
                canonical_json(evidence)
                if (not isinstance(evidence, dict) or evidence.get("quiesced") is not True or
                        evidence.get("safe_to_release") is not True):
                    raise OwnerFault("Quiescence and cleanup must prove the resource reusable")
                with self.journal.transaction() as db:
                    changed = db.execute(
                        "UPDATE leases SET state='released',cleanup=?,version=version+1 "
                        "WHERE lease_id=? AND owner_id=? AND state='reconciling' AND version=?",
                        (canonical_json(evidence), lease_id, self.owner_id, version)).rowcount
                    if changed != 1:
                        raise Conflict("Reservation changed during physical reconciliation")
                return evidence
            except BaseException:
                with self.journal.transaction() as db:
                    db.execute("UPDATE leases SET state='quarantined',version=version+1 "
                               "WHERE lease_id=? AND owner_id=? AND state='reconciling' AND version=?",
                               (lease_id, self.owner_id, version))
                raise
