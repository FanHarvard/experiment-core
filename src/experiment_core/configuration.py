"""Exact, revision-bound configuration adoption for trusted local hosts.

The host supplies human authorization and physical proof/readback adapters.
No boolean or approval claim from a request is accepted as authorization.
prepare() is read-only. adopt() performs one bounded transition; verify() must
report settled=True only after all affected participants can no longer apply an
older in-flight transition. Process fencing never substitutes for that proof.
"""
from copy import deepcopy
from dataclasses import dataclass
import json
import secrets

from experiment_core.errors import AuthorizationRequired, Conflict, StaleContext, UncertainWrite
from experiment_core.records import canonical_json, digest, safe_id


@dataclass(frozen=True)
class Parameter:
    category: str
    unit: str
    group: str
    boundary: str
    safety_affecting: bool = False

    def __post_init__(self):
        if self.category not in {"safety", "calibration", "quality", "operational", "defaults"}:
            raise ValueError("Unrecognized parameter category")
        if self.boundary not in {"cycle", "between_points", "before_arm", "next_run", "idle"}:
            raise ValueError("Unrecognized adoption boundary")


class Configuration:
    def __init__(self, journal, scope, parameters, initial, *, dependencies=None):
        self.journal = journal
        self.scope = safe_id(scope)
        self.parameters = dict(parameters)
        if set(initial) != set(parameters):
            raise ValueError("Initial configuration must cover every registered parameter")
        with journal.transaction() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS configurations (
                scope TEXT PRIMARY KEY, saved TEXT NOT NULL, effective TEXT NOT NULL,
                projection TEXT NOT NULL, value TEXT NOT NULL, dependencies TEXT NOT NULL,
                pending TEXT)""")
            db.execute("""CREATE TABLE IF NOT EXISTS approvals (
                token TEXT PRIMARY KEY, scope TEXT NOT NULL, proposal TEXT NOT NULL,
                receipt TEXT NOT NULL, used INTEGER NOT NULL DEFAULT 0)""")
            db.execute("""CREATE TABLE IF NOT EXISTS configuration_events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT, scope TEXT NOT NULL,
                kind TEXT NOT NULL, body TEXT NOT NULL)""")
            db.execute("CREATE INDEX IF NOT EXISTS configuration_history ON configuration_events(scope,sequence)")
            revision = digest(initial)
            db.execute("INSERT OR IGNORE INTO configurations VALUES (?,?,?,?,?,?,NULL)", (
                scope, revision, revision, revision, canonical_json(initial), canonical_json(dependencies or {})))

    def state(self):
        with self.journal._mutex:
            row = self.journal._db.execute("SELECT * FROM configurations WHERE scope=?", (self.scope,)).fetchone()
        value = dict(row)
        for name in ("value", "dependencies", "pending"):
            value[name] = json.loads(value[name]) if value[name] is not None else None
        return value

    def propose(self, changes, *, base_revision, dependencies):
        changes, dependencies = deepcopy(changes), deepcopy(dependencies)
        current = self.state()
        if current["pending"] is not None:
            raise Conflict("An adoption requires verification or reconciliation")
        if base_revision != current["effective"] or dependencies != current["dependencies"]:
            raise StaleContext("Policy or proof dependencies changed")
        if not changes or not set(changes) <= set(self.parameters):
            raise ValueError("Only registered parameters may be changed")
        groups = {self.parameters[key].group for key in changes if self.parameters[key].group}
        if any(p.group in groups and key not in changes for key, p in self.parameters.items()):
            raise ValueError("Coupled parameters must be proposed together")
        value = {**current["value"], **deepcopy(changes)}
        proposal = {"scope": self.scope, "base_revision": base_revision,
                    "dependencies": deepcopy(dependencies), "changes": deepcopy(changes),
                    "revision": digest(value)}
        canonical_json(proposal)
        return proposal

    def approve(self, proposal, authorize_human):
        """Called only by the trusted host approval route, never by a provider tool."""
        proposal = deepcopy(proposal)
        self._validate(proposal)
        receipt = deepcopy(authorize_human(deepcopy(proposal)))
        if not isinstance(receipt, dict) or not receipt.get("human_identity") or not receipt.get("receipt_id"):
            raise AuthorizationRequired("Trusted human approval receipt required")
        token = secrets.token_hex(32)
        with self.journal.transaction() as db:
            db.execute("INSERT INTO approvals VALUES (?,?,?,?,0)",
                       (token, self.scope, digest(proposal), canonical_json(receipt)))
            self._event(db, "approval", {"proposal": proposal, "receipt_id": receipt["receipt_id"]})
        return token

    def _validate(self, proposal):
        expected = {"scope", "base_revision", "dependencies", "changes", "revision"}
        if not isinstance(proposal, dict) or set(proposal) != expected or proposal["scope"] != self.scope:
            raise ValueError("Malformed configuration proposal")
        rebuilt = self.propose(proposal["changes"], base_revision=proposal["base_revision"],
                               dependencies=proposal["dependencies"])
        if rebuilt != proposal:
            raise StaleContext("Proposal content changed")

    def adopt(self, proposal, adapter, *, boundary, approval_token=None, quality_authorized=False,
              operational_authorized=False):
        proposal = deepcopy(proposal)
        # The process lock spans proof/I/O/readback without a SQLite transaction.
        # A second connection/process cannot reconcile a still-running callback.
        with self.journal.guard("configuration", self.scope):
            return self._adopt(proposal, adapter, boundary=boundary, approval_token=approval_token,
                               quality_authorized=quality_authorized, operational_authorized=operational_authorized)

    def _adopt(self, proposal, adapter, *, boundary, approval_token, quality_authorized, operational_authorized):
        self._validate(proposal)
        changed = [self.parameters[k] for k in proposal["changes"]]
        if any(p.boundary != boundary for p in changed):
            return {"state": "deferred", "reason": "Declared adoption boundary not reached"}
        human = any(p.safety_affecting or p.category not in {"quality", "operational"} for p in changed)
        if not human and any(p.category == "quality" for p in changed) and not quality_authorized:
            raise AuthorizationRequired("Host-granted quality update authority required")
        if not human and any(p.category == "operational" for p in changed) and not operational_authorized:
            raise AuthorizationRequired("Host-granted operational update authority required")
        # Reserve exact state with CAS before proof or I/O; no conflicting adoption races it.
        pending = {"proposal": deepcopy(proposal), "phase": "preparing"}
        with self.journal.transaction() as db:
            if human:
                receipt = db.execute("SELECT * FROM approvals WHERE token=?", (approval_token,)).fetchone()
                if (receipt is None or receipt["used"] or receipt["scope"] != self.scope or
                        receipt["proposal"] != digest(proposal)):
                    raise AuthorizationRequired("Approval must bind the exact diff, revision and dependencies")
            changed_rows = db.execute(
                "UPDATE configurations SET saved=?,pending=? WHERE scope=? AND effective=? AND dependencies=? AND pending IS NULL",
                (proposal["revision"], canonical_json(pending), self.scope, proposal["base_revision"],
                 canonical_json(proposal["dependencies"]))).rowcount
            if changed_rows != 1:
                raise Conflict("Configuration changed during admission")
            if human:
                db.execute("UPDATE approvals SET used=1 WHERE token=?", (approval_token,))
            self._event(db, "prepare_intent", {"proposal": proposal,
                        "approval_receipt": json.loads(receipt["receipt"])["receipt_id"] if human else None})
        try:
            # Application checks current outputs AND admitted future trajectories.
            proof = deepcopy(adapter.prepare(deepcopy(proposal)))
            canonical_json(proof)
        except Exception as error:
            self._clear_pending(pending, {"prepare_error": str(error), "effect": "none"})
            raise
        previous_pending = deepcopy(pending)
        pending.update(phase="adopting", proof=proof)
        self._replace_pending(previous_pending, pending)
        try:
            adapter.adopt(deepcopy(proposal), deepcopy(proof))
            evidence = deepcopy(adapter.verify(deepcopy(proposal)))
            canonical_json(evidence)
            if (evidence.get("settled") is not True or
                    evidence.get("effective_revision") != proposal["revision"] or
                    evidence.get("projection_revision") != proposal["revision"]):
                raise UncertainWrite("Not every affected participant verified the candidate revision")
        except Exception as error:
            previous_pending = deepcopy(pending)
            pending.update(phase="uncertain", reason=str(error))
            self._replace_pending(previous_pending, pending)
            raise UncertainWrite("Adoption requires readback reconciliation; do not replay") from error
        self._publish(proposal, evidence)
        return {"state": "effective", "revision": proposal["revision"], "evidence": evidence}

    def _replace_pending(self, previous, pending):
        with self.journal.transaction() as db:
            changed = db.execute("UPDATE configurations SET pending=? WHERE scope=? AND pending=?",
                                 (canonical_json(pending), self.scope, canonical_json(previous))).rowcount
            if changed != 1:
                raise Conflict("Adoption phase changed during adapter work")
            self._event(db, "adoption_phase", pending)

    def _clear_pending(self, pending, evidence):
        with self.journal.transaction() as db:
            changed = db.execute("UPDATE configurations SET saved=effective,pending=NULL "
                                 "WHERE scope=? AND pending=? AND effective=? AND dependencies=?",
                                 (self.scope, canonical_json(pending), pending["proposal"]["base_revision"],
                                  canonical_json(pending["proposal"]["dependencies"]))).rowcount
            if changed != 1:
                raise Conflict("Adoption changed before confirmed abort")
            self._event(db, "aborted", {"pending": pending, "evidence": evidence})

    def _publish(self, proposal, evidence):
        with self.journal.transaction() as db:
            row = db.execute("SELECT * FROM configurations WHERE scope=?", (self.scope,)).fetchone()
            pending = json.loads(row["pending"]) if row["pending"] else None
            if (pending is None or pending.get("proposal") != proposal or
                    row["effective"] != proposal["base_revision"] or
                    json.loads(row["dependencies"]) != proposal["dependencies"]):
                raise Conflict("Adoption changed during verification")
            value = {**json.loads(row["value"]), **proposal["changes"]}
            db.execute("UPDATE configurations SET saved=?,effective=?,projection=?,value=?,pending=NULL WHERE scope=?",
                       (proposal["revision"], proposal["revision"], proposal["revision"], canonical_json(value), self.scope))
            self._event(db, "effective", {"pending": pending, "evidence": evidence})

    def _event(self, db, kind, body):
        db.execute("INSERT INTO configuration_events(scope,kind,body) VALUES (?,?,?)",
                   (self.scope, kind, canonical_json(body)))

    def history(self, *, after=0, limit=100):
        """Paginated exact approvals, proof and readback retained after publication."""
        self.journal._limit(limit)
        with self.journal._mutex:
            rows = self.journal._db.execute(
                "SELECT sequence,kind,body FROM configuration_events WHERE scope=? AND sequence>? "
                "ORDER BY sequence LIMIT ?", (self.scope, after, limit)).fetchall()
        return [{"sequence": row["sequence"], "kind": row["kind"], "body": json.loads(row["body"])}
                for row in rows]

    def reconcile(self, adapter):
        """Readback only, with settled evidence; never repeats an adoption.

        A verified candidate is published. If every participant confirms the old
        revision and no adoption can still complete, the pending intent is
        aborted. Mixed/unsettled revisions remain pending for owner repair.
        """
        with self.journal.guard("configuration", self.scope):
            return self._reconcile(adapter)

    def _reconcile(self, adapter):
        current = self.state()
        if current["pending"] is None:
            return current
        proposal = current["pending"]["proposal"]
        evidence = deepcopy(adapter.verify(deepcopy(proposal)))
        canonical_json(evidence)
        if not isinstance(evidence, dict) or evidence.get("settled") is not True:
            raise UncertainWrite("Owner has not confirmed that adoption is settled")
        revisions = (evidence.get("effective_revision"), evidence.get("projection_revision"))
        if revisions == (proposal["revision"], proposal["revision"]):
            self._publish(proposal, evidence)
        elif revisions == (proposal["base_revision"], proposal["base_revision"]):
            self._clear_pending(current["pending"], evidence)
        else:
            raise UncertainWrite("Candidate adoption remains unverified")
        return self.state()

    def set_dependencies(self, dependencies):
        """Trusted calibration/configuration integration invalidates pending proofs."""
        dependencies = deepcopy(dependencies)
        with self.journal.transaction() as db:
            changed = db.execute("UPDATE configurations SET dependencies=? WHERE scope=? AND pending IS NULL",
                                 (canonical_json(dependencies), self.scope)).rowcount
            if changed != 1:
                raise Conflict("Cannot change dependencies during adoption")
            self._event(db, "dependencies", {"dependencies": dependencies})
