"""Bounded synchronous executor for trusted adapters and simulated assemblies.

The executor records an intent before every owner call. Reopening a running
intent requires inspection; it never resends the action. Native timing remains
inside the application owner, independent of this decision loop.
"""
from copy import deepcopy
from dataclasses import dataclass, field
import math
import threading
import time

from jsonschema import Draft202012Validator

from experiment_core.errors import (AuthorizationRequired, Cancelled, Conflict, Deadline,
                     OperationError, OwnerFault, PrewriteRefusal, StaleContext, UncertainWrite)
from experiment_core.records import canonical_json, digest, safe_id

TERMINAL = frozenset({"succeeded", "inconclusive", "cancelled", "failed", "uncertain"})


@dataclass(frozen=True)
class Operation:
    operation_id: str
    version: str
    parameters: dict
    result: dict
    resources: frozenset
    execute: object
    cancel: object
    attachments: frozenset = frozenset()


class Registry:
    def __init__(self):
        self._operations = {}

    def register(self, operation):
        operation = deepcopy(operation)
        safe_id(operation.operation_id)
        if not operation.version or not callable(operation.execute) or not callable(operation.cancel):
            raise ValueError("Version, executor and cleanup implementation are required")
        if operation.operation_id in self._operations:
            raise Conflict("Operation ID already registered")
        for schema in (operation.parameters, operation.result):
            Draft202012Validator.check_schema(schema)
            self._local_refs(schema)
        # Registration is explicit trusted code; no import paths arrive in requests.
        self._operations[operation.operation_id] = deepcopy(operation)

    @classmethod
    def _local_refs(cls, value):
        if isinstance(value, dict):
            for key, child in value.items():
                if key in ("$ref", "$dynamicRef") and not child.startswith("#"):
                    raise ValueError("Only local schema references are supported")
                cls._local_refs(child)
        elif isinstance(value, list):
            for child in value:
                cls._local_refs(child)

    def get(self, operation_id, version):
        operation = self._operations[operation_id]
        if operation.version != version:
            raise StaleContext("Unsupported operation version")
        return deepcopy(operation)

    def describe(self):
        return [{"operation_id": op.operation_id, "version": op.version,
                 "resources": sorted(op.resources), "attachments": sorted(op.attachments),
                 "parameters": deepcopy(op.parameters), "result": deepcopy(op.result)}
                for op in self._operations.values()]


@dataclass
class Context:
    grant: object
    deadline: float
    remaining_steps: int
    clock: object = time.monotonic
    parent: object = None
    cancelled: bool = False
    _children: list = field(default_factory=list, repr=False)
    _budget_lock: object = field(default_factory=threading.RLock, repr=False, compare=False)

    def __post_init__(self):
        if (type(self.remaining_steps) is not int or self.remaining_steps < 1 or
                type(self.deadline) not in (int, float) or not math.isfinite(self.deadline)):
            raise ValueError("A finite deadline and positive step budget are required")

    def check(self):
        if self.cancelled or self.parent is not None and self.parent.is_cancelled():
            raise Cancelled("Operation cancelled")
        if self.clock() >= self.deadline or self.remaining_steps <= 0:
            raise Deadline("Operation budget exhausted")

    def is_cancelled(self):
        return self.cancelled or self.parent is not None and self.parent.is_cancelled()

    def consume(self):
        with self._budget_lock:
            self.check()
            # Sibling operations share their parent's finite budget atomically.
            if self.parent is not None:
                self.parent.consume()
            self.remaining_steps -= 1

    def child(self, request_id, resources, *, steps, deadline):
        with self._budget_lock:
            self.check()
            child = Context(self.grant.child(request_id, resources), min(self.deadline, deadline),
                            min(self.remaining_steps, steps), self.clock, self)
            child._budget_lock = self._budget_lock
            self._children.append(child)
            return child


class Executor:
    def __init__(self, registry, journal, owner):
        self.registry = registry
        self.journal = journal
        self.owner = owner

    def execute(self, context, operation_id, version, parameters):
        operation = self.registry.get(operation_id, version)
        parameters = deepcopy(parameters)
        canonical_json(parameters)
        Draft202012Validator(operation.parameters).validate(parameters)
        if not operation.resources <= context.grant.resources:
            raise AuthorizationRequired("Operation exceeds the granted resources")
        request = {"operation_id": operation_id, "version": version, "parameters": deepcopy(parameters),
                   "lease_id": context.grant.lease_id, "generation": context.grant.generation,
                   "revision": context.grant.policy_revision, "owner_id": context.grant.owner_id,
                   "resources": sorted(context.grant.resources)}
        # Serialize bounded adapter calls with cleanup/reconciliation. The owner
        # remains responsible for native/background work and transport fencing.
        with self.journal.guard("owner", context.grant.owner_id):
            return self._execute(context, operation, parameters, request)

    def _execute(self, context, operation, parameters, request):
        request_id = context.grant.request_id
        fresh = self.journal.admit(request_id, context.grant.system_id, request)
        old = self.journal.get(request_id)
        if not fresh:
            if old["state"] in TERMINAL:
                return old
            self.owner.quarantine(context.grant)
            raise UncertainWrite("Existing intent needs owner inspection; automatic replay is forbidden")
        try:
            context.check()
            self.owner.validate(context.grant, operation, deepcopy(parameters))
        except Cancelled as error:
            return self._finish(request_id, 0, "cancelled", {"error": error.as_record(), "effect": "none"})
        except (UncertainWrite, OwnerFault) as error:
            self.owner.quarantine(context.grant)
            return self._finish(request_id, 0, "uncertain", {"error": error.as_record(), "effect": "unknown"})
        except OperationError as error:
            return self._finish(request_id, 0, "failed", {"error": error.as_record(), "effect": "none"})
        sequence = self.journal.transition(request_id, 0, "running", {"kind": "intent"})
        try:
            context.consume()
            result = deepcopy(operation.execute(self.owner, context, deepcopy(parameters)))
            canonical_json(result)
            Draft202012Validator(operation.result).validate(result)
            # The owner supplies scientific outcome separately from successful execution.
            return self._finish(request_id, sequence, "succeeded", {"value": result, "effect": "confirmed"})
        except PrewriteRefusal as error:
            return self._finish(request_id, sequence, "inconclusive", {"error": error.as_record(), "effect": "none"})
        except BaseException as error:
            # Even serialization/validation/crash after dispatch may hide a physical effect.
            self.owner.quarantine(context.grant)
            record = {"code": getattr(error, "code", "unclassified_after_dispatch"), "message": str(error)}
            self._finish(request_id, sequence, "uncertain", {"error": record, "effect": "unknown"})
            if not isinstance(error, Exception):
                raise
            return self.journal.get(request_id)

    def _finish(self, request_id, sequence, state, result):
        self.journal.transition(request_id, sequence, state, {"kind": "outcome", "state": state}, result)
        return self.journal.get(request_id)

    def cancel(self, context, operation_id, version):
        """Record and deduplicate cleanup, independently of observation admission.

        Cancellation is cooperative between bounded steps. A busy owner refuses
        cleanup until its current step returns; the context is cancelled at once.
        An interrupted cleanup must be inspected, never automatically replayed.
        """
        operation = self.registry.get(operation_id, version)
        if not operation.resources <= context.grant.resources:
            raise AuthorizationRequired("Cancellation exceeds grant scope")
        context.cancelled = True
        request_id = "cancel." + digest([context.grant.system_id, context.grant.request_id])
        request = {"kind": "cancel", "target_request_id": context.grant.request_id,
                   "operation_id": operation_id, "version": version,
                   "owner_id": context.grant.owner_id, "generation": context.grant.generation,
                   "lease_id": context.grant.lease_id, "resources": sorted(context.grant.resources)}
        with self.journal.guard("owner", context.grant.owner_id):
            try:
                target = self.journal.get(context.grant.request_id)["request"]
            except KeyError:
                target = None
            if target is not None and (target.get("operation_id") != operation_id or
                                       target.get("version") != version):
                raise Conflict("Cancellation must identify the admitted operation")
            try:
                self.journal.get(request_id)
            except KeyError:
                # Observation/policy revisions may be stale; owner identity and
                # the reservation being cancelled must still be authoritative.
                self.owner.validate_cancel(context.grant, operation)
            fresh = self.journal.admit(request_id, context.grant.system_id, request)
            old = self.journal.get(request_id)
            if not fresh:
                if old["state"] == "cancelled":
                    return old["result"]
                self.owner.quarantine(context.grant)
                raise UncertainWrite("Existing cleanup intent requires inspection; replay is forbidden")
            sequence = self.journal.transition(request_id, 0, "cancelling", {"kind": "cleanup_intent"})
            try:
                # Application cleanup must preserve a parent lock for attached work.
                evidence = deepcopy(operation.cancel(self.owner, context))
                canonical_json(evidence)
                if not isinstance(evidence, dict) or evidence.get("confirmed") is not True:
                    raise UncertainWrite("Cleanup is not confirmed")
                result = {"state": "cancelled", "cleanup": evidence}
                self._finish(request_id, sequence, "cancelled", result)
                return result
            except BaseException as error:
                self.owner.quarantine(context.grant)
                self._finish(request_id, sequence, "uncertain", {
                    "error": {"code": getattr(error, "code", "uncertain_cleanup"), "message": str(error)},
                    "effect": "unknown"})
                if not isinstance(error, Exception):
                    raise
                raise UncertainWrite("Cleanup requires readback reconciliation; do not replay") from error

    def reconcile(self, context, inspect, *, cancellation=False):
        """Publish a trusted owner's settled readback, without dispatch or release.

        The inspector receives the durable request and must account for native
        work that could still complete. Resource release is a separate fenced
        Reservations.reconcile transaction. Providers cannot supply this adapter.
        """
        request_id = ("cancel." + digest([context.grant.system_id, context.grant.request_id])
                      if cancellation else context.grant.request_id)
        with self.journal.guard("owner", context.grant.owner_id):
            old = self.journal.get(request_id)
            request = old["request"]
            if (old["scope"] != context.grant.system_id or
                    request.get("owner_id") != context.grant.owner_id or
                    request.get("lease_id") != context.grant.lease_id or
                    request.get("generation") != context.grant.generation or
                    request.get("resources") != sorted(context.grant.resources)):
                raise AuthorizationRequired("Reconciliation must identify the original scoped request")
            if old["state"] in TERMINAL and old["state"] != "uncertain":
                return old
            evidence = deepcopy(inspect(deepcopy(old)))
            canonical_json(evidence)
            if (not isinstance(evidence, dict) or evidence.get("settled") is not True or
                    evidence.get("effect") not in {"none", "confirmed"}):
                raise UncertainWrite("Owner readback has not settled the action's effects")
            state = evidence.get("state")
            if cancellation:
                if state != "cancelled" or evidence.get("cleanup", {}).get("confirmed") is not True:
                    raise UncertainWrite("Cancellation cleanup remains unconfirmed")
            elif state not in TERMINAL - {"uncertain"}:
                raise ValueError("Inspection must identify a terminal outcome")
            elif state == "succeeded":
                operation = self.registry.get(request["operation_id"], request["version"])
                Draft202012Validator(operation.result).validate(evidence.get("value"))
                if evidence["effect"] != "confirmed":
                    raise UncertainWrite("Success requires confirmed owner evidence")
            self.journal.transition(request_id, old["sequence"], state,
                                    {"kind": "reconciliation", "state": state}, evidence)
            return self.journal.get(request_id)


class ManualDecisions:
    """A host UI delivers decisions; an empty queue parks the subordinate run."""
    def __init__(self, decisions=()):
        self.decisions = list(deepcopy(decisions))

    def propose(self, measurement, capabilities, context):
        return self.decisions.pop(0) if self.decisions else None


def decide(provider, evidence, actions, context):
    """Bounded proposal validation. An adapter must impose provider I/O timeout.

    Providers receive copies, never a grant, transport or mutable Context.
    Every selected action still passes through Executor and owner validation.
    """
    context.check()
    proposal = provider.propose(deepcopy(evidence), deepcopy(actions), {
        "policy_revision": context.grant.policy_revision,
        "remaining_steps": context.remaining_steps, "deadline": context.deadline})
    context.check()
    if proposal is None:
        return {"state": "waiting_for_decision"}
    canonical_json(proposal)
    if not isinstance(proposal, dict) or set(proposal) != {"action", "parameters"}:
        raise ValueError("Expected action and parameters")
    if proposal["action"] not in actions:
        raise AuthorizationRequired("Decision selects an unavailable action")
    Draft202012Validator(actions[proposal["action"]]).validate(proposal["parameters"])
    return deepcopy(proposal)
