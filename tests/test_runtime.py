from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import os
from pathlib import Path

import pytest

import experiment_core
from experiment_core.configuration import Configuration, Parameter
from experiment_core.errors import (AuthorizationRequired, Conflict, Deadline, OwnerFault,
                                    StaleContext, UncertainWrite)
from experiment_core.execution import Context, Executor, ManualDecisions, Registry, decide
from experiment_core.ownership import Reservations
from experiment_core.records import FileArtifacts, Journal, atomic_json, read_json
from experiment_core.testing import SimulatedStage, register_stage


def child_environment():
    return {**os.environ, "PYTHONPATH": str(Path(experiment_core.__file__).resolve().parent.parent)}


def test_retired_owner_cannot_acquire_new_resources_or_run_cleanup(tmp_path):
    path = tmp_path / "owners.sqlite3"
    first, second = Journal(path), Journal(path)
    try:
        old = Reservations(first, "physical-owner")
        grant = old.acquire("assembly", "old-request", {"stage/x"}, "r1", ttl_s=20)
        replacement = Reservations(second, "physical-owner")
        with pytest.raises(StaleContext):
            old.acquire("assembly", "new-request", {"stage/y"}, "r1", ttl_s=20)
        with pytest.raises(StaleContext):
            old.validate_cleanup(grant)
        called = []
        with pytest.raises(StaleContext):
            old.reconcile(grant.lease_id, lambda fence: called.append(fence))
        assert called == []
        replacement.reconcile(grant.lease_id, lambda fence: {"quiesced": True, "safe_to_release": True})
        fresh = replacement.acquire("assembly", "fresh-request", {"stage/x"}, "r1", ttl_s=20)
        with pytest.raises(StaleContext):
            old.quarantine(fresh)
        replacement.validate(fresh, policy_revision="r1")
    finally:
        first.close()
        second.close()


@pytest.fixture
def runtime(tmp_path):
    journal = Journal(tmp_path / "journal.sqlite3")
    registry = Registry()
    register_stage(registry)
    clock = [10.0]
    leases = Reservations(journal, "fixture-owner", clock=lambda: clock[0])
    owner = SimulatedStage(leases)
    grant = leases.acquire("assembly", "move-1", {"stage/x", "stage/y"}, "fixture-v1", ttl_s=20)
    context = Context(grant, 25, 10, clock=lambda: clock[0])
    yield journal, registry, leases, owner, context, clock
    journal.close()


def test_duplicate_request_returns_result_without_actuation(runtime):
    journal, registry, _, owner, context, _ = runtime
    executor = Executor(registry, journal, owner)
    first = executor.execute(context, "fixture.move", "1", {"x": 1, "y": 2})
    assert first["state"] == "succeeded"
    assert first == executor.execute(context, "fixture.move", "1", {"x": 1, "y": 2})
    assert owner.writes == 1
    with pytest.raises(Conflict):
        executor.execute(context, "fixture.move", "1", {"x": 2, "y": 2})


def test_restart_keeps_inflight_intent_and_quarantines_lease(runtime, tmp_path):
    journal, registry, _, owner, context, _ = runtime
    request = {"operation_id": "fixture.move", "version": "1", "parameters": {"x": 1, "y": 2},
               "lease_id": context.grant.lease_id, "generation": context.grant.generation, "revision": "fixture-v1",
               "owner_id": context.grant.owner_id, "resources": sorted(context.grant.resources)}
    journal.admit("move-1", "assembly", request)
    journal.transition("move-1", 0, "running", {"kind": "intent"})
    reopened = Journal(tmp_path / "journal.sqlite3")
    try:
        replacement = Reservations(reopened, "fixture-owner", clock=lambda: 11)
        with pytest.raises(UncertainWrite):
            Executor(registry, reopened, owner).execute(context, "fixture.move", "1", {"x": 1, "y": 2})
        with pytest.raises(StaleContext):
            replacement.validate(context.grant, policy_revision="fixture-v1")
        with pytest.raises(Conflict):
            replacement.acquire("assembly", "new", {"stage/x"}, "fixture-v1", ttl_s=20)
        assert owner.writes == 0
    finally:
        reopened.close()


def test_unknown_write_prevents_replay_and_resource_reuse(runtime):
    journal, registry, leases, owner, context, _ = runtime
    def disconnect(context, parameters):
        owner.writes += 1
        raise OSError("reply lost after output")
    owner.move = disconnect
    executor = Executor(registry, journal, owner)
    first = executor.execute(context, "fixture.move", "1", {"x": 1, "y": 2})
    assert first["state"] == "uncertain"
    assert first["result"]["effect"] == "unknown"
    assert executor.execute(context, "fixture.move", "1", {"x": 1, "y": 2}) == first
    assert owner.writes == 1
    with pytest.raises(Conflict):
        leases.acquire("other", "other-request", {"stage/x"}, "v1", ttl_s=1)


def test_confirmed_prewrite_rejection_and_expired_context_do_not_write(runtime):
    journal, registry, _, owner, context, clock = runtime
    record = Executor(registry, journal, owner).execute(context, "fixture.move", "1", {"x": 20, "y": 2})
    assert record["result"]["effect"] == "none"
    clock[0] = 26
    context.grant = replace(context.grant, request_id="late")
    record = Executor(registry, journal, owner).execute(context, "fixture.move", "1", {"x": 2, "y": 2})
    assert record["result"]["error"]["code"] == "deadline"
    assert owner.writes == 0


def test_child_cancel_and_provider_failure_preserve_primary(runtime):
    journal, registry, _, owner, context, clock = runtime
    child = context.child("search", context.grant.resources, steps=2, deadline=20)
    clock[0] = 30  # cancellation still works with expired observation/deadline context
    result = Executor(registry, journal, owner).cancel(child, "fixture.move", "1")
    assert result["cleanup"]["parent_running"] is True
    assert child.cancelled and not context.cancelled
    clock[0] = 12
    class BrokenProvider:
        def propose(self, *args):
            raise OSError("provider unavailable")
    with pytest.raises(OSError):
        decide(BrokenProvider(), {}, {}, context)
    assert owner.primary_running and not context.cancelled


def test_child_cannot_expand_resources_and_consumes_parent_budget(runtime):
    *_, context, clock = runtime
    with pytest.raises(AuthorizationRequired):
        context.child("bad", {"other-resource"}, steps=2, deadline=20)
    child = context.child("child", {"stage/x"}, steps=2, deadline=20)
    child.consume()
    assert context.remaining_steps == 9
    child.consume()
    with pytest.raises(Deadline):
        child.consume()


@pytest.mark.parametrize("mode", ["deterministic", "manual", "resident"])
def test_nonlaser_providers_share_executor(runtime, mode):
    journal, registry, _, owner, context, _ = runtime
    action = {"action": "fixture.move", "parameters": {"x": 2, "y": 3}}
    class Provider:
        def propose(self, evidence, actions, remaining):
            assert "lease_id" not in remaining
            return action
    provider = ManualDecisions([action]) if mode == "manual" else Provider()
    proposal = decide(provider, {"position": owner.position},
                      {"fixture.move": registry.get("fixture.move", "1").parameters}, context)
    result = Executor(registry, journal, owner).execute(context, proposal["action"], "1", proposal["parameters"])
    assert result["result"]["value"]["position"] == {"x": 2, "y": 3}


def test_waiting_manual_and_late_decision_obey_deadline(runtime):
    *_, context, clock = runtime
    assert decide(ManualDecisions(), {}, {}, context) == {"state": "waiting_for_decision"}
    class SlowProvider:
        def propose(self, *args):
            clock[0] = 30
            return {"action": "anything", "parameters": {}}
    with pytest.raises(Deadline):
        decide(SlowProvider(), {}, {}, context)


def test_expired_lease_requires_cleanup_evidence(runtime):
    _, _, leases, _, context, clock = runtime
    clock[0] = 31
    with pytest.raises(OwnerFault):
        leases.validate(context.grant, policy_revision="fixture-v1")
    with pytest.raises(OwnerFault):
        leases.reconcile(context.grant.lease_id, lambda _: {"safe_to_release": False})
    with pytest.raises(Conflict):
        leases.acquire("other", "r2", {"stage/x"}, "v1", ttl_s=20)
    leases.reconcile(context.grant.lease_id, lambda _: {
        "quiesced": True, "safe_to_release": True, "readback": [1, 2]})
    leases.acquire("other", "r2", {"stage/x"}, "v1", ttl_s=20)


def test_device_coupling_is_declared_by_adapter(tmp_path):
    journal = Journal(tmp_path / "journal.sqlite3")
    leases = Reservations(journal, "shared", expand_resources=lambda r: r | {"device/shared-clock"})
    leases.acquire("one", "r1", {"device/ch1"}, "v1", ttl_s=10)
    with pytest.raises(Conflict):
        leases.acquire("two", "r2", {"device/ch2"}, "v1", ttl_s=10)
    journal.close()


def test_concurrent_request_id_and_cas(tmp_path):
    journal = Journal(tmp_path / "journal.sqlite3")
    with ThreadPoolExecutor(8) as pool:
        created = list(pool.map(lambda _: journal.admit("same", "scope", {"v": 1}), range(20)))
    assert sum(created) == 1
    journal.transition("same", 0, "running", {"intent": True})
    with pytest.raises(Conflict):
        journal.transition("same", 0, "running", {"intent": True})
    assert len(journal.events("same")) == 1
    for i in range(10):
        journal.admit("request-%02d" % i, "scope", {})
    first = journal.list("scope", limit=3)
    second = journal.list("scope", after=first[-1]["id"], limit=3)
    assert len(first) == len(second) == 3
    assert first[-1]["id"] < second[0]["id"]
    journal.close()


def test_artifact_integrity_and_failed_json_publication(tmp_path):
    store = FileArtifacts(tmp_path / "artifacts")
    reference = store.put("scope", b"raw evidence")
    assert store.put("scope", b"raw evidence") == reference
    assert store.read("scope", reference["artifact_id"]) == b"raw evidence"
    with pytest.raises(ValueError):
        store.read("../outside", reference["artifact_id"])
    (tmp_path / "artifacts" / "scope" / reference["artifact_id"]).write_bytes(b"damaged")
    with pytest.raises(Conflict):
        store.read("scope", reference["artifact_id"])
    path = tmp_path / "record.json"
    atomic_json(path, {"v": 1})
    with pytest.raises(ValueError):
        atomic_json(path, {"v": float("nan")})
    assert read_json(path) == {"v": 1}


@pytest.fixture
def config(tmp_path):
    journal = Journal(tmp_path / "config.sqlite3")
    params = {"minimum": Parameter("safety", "mm", "range", "idle"),
              "maximum": Parameter("safety", "mm", "range", "idle"),
              "quality": Parameter("quality", "mm", "", "cycle")}
    config = Configuration(journal, "stage", params, {"minimum": -10, "maximum": 10, "quality": 1},
                           dependencies={"calibration": "v1"})
    yield config
    journal.close()


class Adoption:
    def __init__(self):
        self.revision = None
        self.writes = 0
        self.reply_lost = False

    def prepare(self, proposal):
        return {"current_and_future_trajectories_checked": True}

    def adopt(self, proposal, proof):
        self.writes += 1
        self.revision = proposal["revision"]
        if self.reply_lost:
            raise OSError("reply lost")

    def verify(self, proposal):
        return {"settled": True, "effective_revision": self.revision, "projection_revision": self.revision}


def proposal(config, changes):
    state = config.state()
    return config.propose(changes, base_revision=state["effective"], dependencies=state["dependencies"])


def approve(config, candidate):
    return config.approve(candidate, lambda _: {"human_identity": "test-human", "receipt_id": "approval-1"})


def test_safety_requires_exact_human_diff_and_group(config):
    with pytest.raises(ValueError):
        proposal(config, {"maximum": 5})
    candidate = proposal(config, {"minimum": -5, "maximum": 5})
    adapter = Adoption()
    with pytest.raises(AuthorizationRequired):
        config.adopt(candidate, adapter, boundary="idle", quality_authorized=True)
    token = approve(config, candidate)
    different = proposal(config, {"minimum": -4, "maximum": 4})
    with pytest.raises(AuthorizationRequired):
        config.adopt(different, adapter, boundary="idle", approval_token=token)
    assert config.adopt(candidate, adapter, boundary="cycle", approval_token=token)["state"] == "deferred"
    assert adapter.writes == 0
    assert config.adopt(candidate, adapter, boundary="idle", approval_token=token)["state"] == "effective"


def test_dependency_change_invalidates_approval(config):
    candidate = proposal(config, {"minimum": -5, "maximum": 5})
    token = approve(config, candidate)
    config.set_dependencies({"calibration": "v2"})
    with pytest.raises(StaleContext):
        config.adopt(candidate, Adoption(), boundary="idle", approval_token=token)


def test_quality_does_not_require_human_but_requires_host_authority(config):
    candidate = proposal(config, {"quality": 0.5})
    with pytest.raises(AuthorizationRequired):
        config.adopt(candidate, Adoption(), boundary="cycle")
    config.adopt(candidate, Adoption(), boundary="cycle", quality_authorized=True)
    assert config.state()["value"]["quality"] == .5


def test_registered_operational_changes_require_their_own_host_authority(tmp_path):
    journal = Journal(tmp_path / "operational.sqlite3")
    try:
        config = Configuration(journal, "settings", {
            "label": Parameter("operational", "", "", "cycle"),
            "gain": Parameter("operational", "", "", "cycle", safety_affecting=True),
        }, {"label": "initial", "gain": 1})
        candidate = proposal(config, {"label": "new"})
        with pytest.raises(AuthorizationRequired, match="operational"):
            config.adopt(candidate, Adoption(), boundary="cycle", quality_authorized=True)
        config.adopt(candidate, Adoption(), boundary="cycle", operational_authorized=True)
        candidate = proposal(config, {"gain": 2})
        with pytest.raises(AuthorizationRequired, match="Approval"):
            config.adopt(candidate, Adoption(), boundary="cycle", operational_authorized=True)
        config.adopt(candidate, Adoption(), boundary="cycle", approval_token=approve(config, candidate))
        assert config.state()["value"] == {"label": "new", "gain": 2}
    finally:
        journal.close()


def test_unknown_adoption_reconciles_without_replay(config):
    candidate = proposal(config, {"minimum": -5, "maximum": 5})
    adapter = Adoption()
    adapter.reply_lost = True
    with pytest.raises(UncertainWrite):
        config.adopt(candidate, adapter, boundary="idle", approval_token=approve(config, candidate))
    state = config.state()
    assert state["saved"] == candidate["revision"] != state["effective"]
    with pytest.raises(Conflict):
        proposal(config, {"quality": .5})
    assert config.reconcile(adapter)["effective"] == candidate["revision"]
    assert adapter.writes == 1


def test_registry_rejects_untrusted_schema_discovery(runtime):
    _, registry, *_ = runtime
    operation = registry.get("fixture.move", "1")
    with pytest.raises(Conflict):
        registry.register(operation)
    with pytest.raises(ValueError):
        Registry().register(replace(operation, parameters={"$ref": "https://untrusted/schema"}))


def test_observation_loss_after_output_never_claims_no_effect(runtime):
    from experiment_core.errors import ObservationUnavailable
    journal, registry, leases, owner, context, _ = runtime

    def move_then_lose_observation(context, parameters):
        owner.position.update(parameters)
        owner.writes += 1
        raise ObservationUnavailable("camera frame unavailable after move")

    owner.move = move_then_lose_observation
    result = Executor(registry, journal, owner).execute(context, "fixture.move", "1", {"x": 1, "y": 2})
    assert result["state"] == "uncertain"
    assert result["result"]["effect"] == "unknown"
    with pytest.raises(OwnerFault):
        leases.validate(context.grant, policy_revision="fixture-v1")
    assert owner.writes == 1


def test_cancel_is_durable_and_duplicate_cleanup_never_repeats(runtime):
    from experiment_core.execution import Registry
    journal, original, _, owner, context, _ = runtime
    calls = []
    registry = Registry()
    operation = original.get("fixture.move", "1")
    registry.register(replace(operation, cancel=lambda *args: calls.append("cleanup") or {"confirmed": True}))
    executor = Executor(registry, journal, owner)
    first = executor.cancel(context, "fixture.move", "1")
    assert first == executor.cancel(context, "fixture.move", "1")
    assert calls == ["cleanup"]
    records = journal.list("assembly")
    assert len(records) == 1 and records[0]["state"] == "cancelled"
    assert journal.events(records[0]["id"])[0]["event"]["kind"] == "cleanup_intent"


def test_interrupted_cancellation_is_quarantined_and_never_replayed(runtime):
    journal, original, leases, owner, context, _ = runtime
    calls = []

    def interrupted_cleanup(*args):
        calls.append("output")
        raise KeyboardInterrupt("process interruption after cleanup output")

    registry = Registry()
    registry.register(replace(original.get("fixture.move", "1"), cancel=interrupted_cleanup))
    executor = Executor(registry, journal, owner)
    with pytest.raises(KeyboardInterrupt):
        executor.cancel(context, "fixture.move", "1")
    assert journal.list("assembly")[0]["state"] == "uncertain"
    with pytest.raises(UncertainWrite):
        executor.cancel(context, "fixture.move", "1")
    with pytest.raises(OwnerFault):
        leases.validate(context.grant, policy_revision="fixture-v1")
    assert calls == ["output"]


def test_cancel_checks_owner_generation_and_released_scope_but_not_old_policy(runtime):
    journal, registry, leases, owner, context, clock = runtime
    stale = replace(context.grant, generation="old-owner-generation")
    with pytest.raises(StaleContext):
        Executor(registry, journal, owner).cancel(replace(context, grant=stale), "fixture.move", "1")
    # Policy revision and deadline are intentionally not cleanup admission gates.
    owner.revision = "new-policy"
    clock[0] = 100
    assert Executor(registry, journal, owner).cancel(context, "fixture.move", "1")["state"] == "cancelled"
    leases.reconcile(context.grant.lease_id, lambda snapshot: {"quiesced": True, "safe_to_release": True})
    another = replace(context, grant=replace(context.grant, request_id="another-cancel"))
    with pytest.raises(OwnerFault):
        Executor(registry, journal, owner).cancel(another, "fixture.move", "1")


def test_active_step_fences_cleanup_and_reconciliation_across_connections(runtime, tmp_path):
    from threading import Event
    journal, registry, leases, owner, context, _ = runtime
    entered, proceed = Event(), Event()
    original_move = owner.move

    def blocked(context, parameters):
        entered.set()
        assert proceed.wait(5)
        return original_move(context, parameters)

    owner.move = blocked
    reopened = Journal(tmp_path / "journal.sqlite3")
    inspected = []
    try:
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(Executor(registry, journal, owner).execute,
                                 context, "fixture.move", "1", {"x": 1, "y": 2})
            try:
                assert entered.wait(5)
                with pytest.raises(Conflict):
                    leases.reconcile(context.grant.lease_id, lambda _: inspected.append(True))
                with pytest.raises(Conflict):
                    Executor(registry, reopened, owner).cancel(context, "fixture.move", "1")
                with pytest.raises(Conflict):
                    Reservations(reopened, "fixture-owner")
                assert context.cancelled and not inspected
            finally:
                proceed.set()
            assert future.result(timeout=5)["state"] == "succeeded"
        assert Executor(registry, reopened, owner).cancel(context, "fixture.move", "1")["state"] == "cancelled"
    finally:
        reopened.close()


def test_reconciliation_requires_quiescence_and_fresh_fence(runtime):
    _, _, leases, _, context, _ = runtime
    with pytest.raises(OwnerFault):
        leases.reconcile(context.grant.lease_id, lambda _: {"safe_to_release": True})

    def competing_fault(snapshot):
        assert snapshot["fence_version"] > snapshot["version"]
        leases.quarantine(context.grant)
        return {"quiesced": True, "safe_to_release": True}

    with pytest.raises(Conflict):
        leases.reconcile(context.grant.lease_id, competing_fault)
    with pytest.raises(Conflict):
        leases.acquire("another", "another", {"stage/x"}, "v1", ttl_s=20)
    evidence = leases.reconcile(context.grant.lease_id, lambda _: {
        "quiesced": True, "safe_to_release": True, "readback": {"stopped": True}})
    assert leases.reconcile(context.grant.lease_id, lambda _: pytest.fail("duplicate cleanup")) == evidence


def test_physical_resource_exclusion_crosses_owner_ids(tmp_path):
    journal = Journal(tmp_path / "owners.sqlite3")
    first, second = Reservations(journal, "one"), Reservations(journal, "two")
    try:
        first.acquire("one", "r1", {"physical/stage/x"}, "v1", ttl_s=20)
        with pytest.raises(Conflict):
            second.acquire("two", "r2", {"physical/stage/x"}, "v1", ttl_s=20)
    finally:
        journal.close()


def test_concurrent_adoption_cannot_be_published_by_reconcile(config):
    from threading import Event
    entered, proceed = Event(), Event()

    class SlowAdoption(Adoption):
        def adopt(self, candidate, proof):
            super().adopt(candidate, proof)
            entered.set()
            assert proceed.wait(5)

    adapter = SlowAdoption()
    candidate = proposal(config, {"minimum": -5, "maximum": 5})
    token = approve(config, candidate)
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(config.adopt, candidate, adapter, boundary="idle", approval_token=token)
        try:
            assert entered.wait(5)
            # Candidate readback is already visible, but the adopting callback
            # still owns the transaction and could produce another effect.
            with pytest.raises(Conflict):
                config.reconcile(adapter)
            with pytest.raises(Conflict):
                config.set_dependencies({"calibration": "v2"})
            assert config.state()["pending"]["phase"] == "adopting"
        finally:
            proceed.set()
        assert future.result(timeout=5)["state"] == "effective"
    assert config.state()["pending"] is None
    assert adapter.writes == 1


@pytest.mark.parametrize("after_output", [False, True])
def test_crashed_adoption_reconciles_old_or_new_revision_without_replay(config, after_output):
    class CrashedAdoption(Adoption):
        def prepare(self, candidate):
            if not after_output:
                raise KeyboardInterrupt("crashed before adoption")
            return super().prepare(candidate)

        def adopt(self, candidate, proof):
            super().adopt(candidate, proof)
            raise KeyboardInterrupt("crashed after adoption")

    before = config.state()["effective"]
    adapter = CrashedAdoption()
    adapter.revision = before
    candidate = proposal(config, {"minimum": -5, "maximum": 5})
    token = approve(config, candidate)
    with pytest.raises(KeyboardInterrupt):
        config.adopt(candidate, adapter, boundary="idle", approval_token=token)
    assert config.state()["pending"] is not None
    recovered = config.reconcile(adapter)
    assert recovered["pending"] is None
    assert recovered["effective"] == (candidate["revision"] if after_output else before)
    assert adapter.writes == int(after_output)
    if not after_output:
        with pytest.raises(AuthorizationRequired):
            config.adopt(candidate, adapter, boundary="idle", approval_token=token)


def test_unsettled_adoption_readback_cannot_publish_or_abort(config):
    class Unsettled(Adoption):
        def verify(self, candidate):
            return {**super().verify(candidate), "settled": False}

    candidate = proposal(config, {"quality": .25})
    adapter = Unsettled()
    with pytest.raises(UncertainWrite):
        config.adopt(candidate, adapter, boundary="cycle", quality_authorized=True)
    with pytest.raises(UncertainWrite):
        config.reconcile(adapter)
    assert config.state()["pending"] is not None
    assert adapter.writes == 1


def test_sibling_operations_share_one_atomic_parent_budget(runtime):
    *_, context, _ = runtime
    context.remaining_steps = 1
    children = [context.child("child-%d" % index, {"stage/x"}, steps=1, deadline=20)
                for index in range(20)]

    def consume(child):
        try:
            child.consume()
            return True
        except Deadline:
            return False

    with ThreadPoolExecutor(8) as pool:
        assert sum(pool.map(consume, children)) == 1
    assert context.remaining_steps == 0


def test_reconciled_request_preserves_evidence_without_replay_or_lease_release(runtime):
    journal, registry, leases, owner, context, _ = runtime

    def interrupted(context, parameters):
        owner.position.update(parameters)
        owner.writes += 1
        raise OSError("reply lost")

    owner.move = interrupted
    executor = Executor(registry, journal, owner)
    executor.execute(context, "fixture.move", "1", {"x": 2, "y": 3})
    result = executor.reconcile(context, lambda record: {
        "settled": True, "state": "succeeded", "effect": "confirmed",
        "value": {"position": dict(owner.position), "objective_achieved": True, "quality": "not_configured"}})
    assert result["state"] == "succeeded"
    assert journal.events(context.grant.request_id)[-1]["event"]["kind"] == "reconciliation"
    assert executor.execute(context, "fixture.move", "1", {"x": 2, "y": 3}) == result
    with pytest.raises(Conflict):
        leases.acquire("next", "next", {"stage/x"}, "v1", ttl_s=20)
    assert owner.writes == 1


def test_real_process_death_preserves_cleanup_intent_and_releases_only_host_lock(runtime, tmp_path):
    import subprocess
    import sys
    from dataclasses import asdict
    journal, registry, leases, owner, context, _ = runtime
    grant = asdict(context.grant)
    grant["resources"] = sorted(grant["resources"])
    marker = tmp_path / "cleanup-output"
    script = '''
import json, os, sys
from dataclasses import replace
from pathlib import Path
from experiment_core.records import Journal
from experiment_core.ownership import Grant
from experiment_core.execution import Context, Executor, Registry
from experiment_core.testing import register_stage
journal = Journal(sys.argv[1])
raw = json.loads(sys.argv[2]); raw['resources'] = frozenset(raw['resources'])
context = Context(Grant(**raw), 100, 1, clock=lambda: 1)
original = Registry(); register_stage(original)
def cleanup(*args):
    Path(sys.argv[3]).write_text('one physical cleanup')
    os._exit(23)
registry = Registry()
registry.register(replace(original.get('fixture.move', '1'), cancel=cleanup))
class Owner:
    def validate_cancel(self, *args): pass
Executor(registry, journal, Owner()).cancel(context, 'fixture.move', '1')
'''
    result = subprocess.run([sys.executable, "-c", script, str(tmp_path / "journal.sqlite3"),
                             json.dumps(grant), str(marker)], capture_output=True, text=True, timeout=5, env=child_environment())
    assert result.returncode == 23, result.stderr
    assert marker.read_text() == "one physical cleanup"
    assert journal.list("assembly")[0]["state"] == "cancelling"
    executor = Executor(registry, journal, owner)
    with pytest.raises(UncertainWrite):
        executor.cancel(context, "fixture.move", "1")
    with pytest.raises(OwnerFault):
        leases.validate(context.grant, policy_revision="fixture-v1")
    settled = executor.reconcile(context, lambda record: {
        "settled": True, "state": "cancelled", "effect": "confirmed",
        "cleanup": {"confirmed": True, "parent_running": True}}, cancellation=True)
    assert settled["state"] == "cancelled"
    assert executor.cancel(context, "fixture.move", "1")["cleanup"]["confirmed"] is True
    assert marker.read_text() == "one physical cleanup"


def test_real_process_death_during_config_prepare_can_abort_without_replay(config, tmp_path):
    import subprocess
    import sys
    script = '''
import os, sys
from experiment_core.configuration import Configuration, Parameter
from experiment_core.records import Journal
parameters = {'minimum': Parameter('safety', 'mm', 'range', 'idle'),
              'maximum': Parameter('safety', 'mm', 'range', 'idle'),
              'quality': Parameter('quality', 'mm', '', 'cycle')}
config = Configuration(Journal(sys.argv[1]), 'stage', parameters,
                       {'minimum': -10, 'maximum': 10, 'quality': 1}, dependencies={'calibration': 'v1'})
state = config.state()
candidate = config.propose({'quality': .5}, base_revision=state['effective'], dependencies=state['dependencies'])
class Adapter:
    def prepare(self, proposal): os._exit(24)
config.adopt(candidate, Adapter(), boundary='cycle', quality_authorized=True)
'''
    before = config.state()["effective"]
    result = subprocess.run([sys.executable, "-c", script, str(tmp_path / "config.sqlite3")],
                            capture_output=True, text=True, timeout=5, env=child_environment())
    assert result.returncode == 24, result.stderr
    assert config.state()["pending"]["phase"] == "preparing"
    adapter = Adoption()
    adapter.revision = before
    recovered = config.reconcile(adapter)
    assert recovered["effective"] == recovered["saved"] == before
    assert recovered["pending"] is None
    assert adapter.writes == 0


def test_approval_binds_reviewed_snapshot_when_callers_mutate_their_proposal(config):
    candidate = proposal(config, {"minimum": -5, "maximum": 5})
    different = proposal(config, {"minimum": -4, "maximum": 4})
    reviewed = []

    def human_receipt(exact_proposal):
        reviewed.append(exact_proposal)
        candidate.clear()
        candidate.update(different)
        return {"human_identity": "human", "receipt_id": "exact-reviewed-diff"}

    token = config.approve(candidate, human_receipt)
    with pytest.raises(AuthorizationRequired):
        config.adopt(candidate, Adoption(), boundary="idle", approval_token=token)
    assert config.adopt(reviewed[0], Adoption(), boundary="idle", approval_token=token)["state"] == "effective"


def test_adoption_and_execution_use_their_admitted_parameter_snapshot(config, runtime):
    candidate = proposal(config, {"minimum": -5, "maximum": 5})
    original = json.loads(json.dumps(candidate))
    token = approve(config, candidate)

    class MutatingCaller(Adoption):
        def prepare(self, reviewed):
            candidate["changes"]["maximum"] = 100
            return super().prepare(reviewed)

    config.adopt(candidate, MutatingCaller(), boundary="idle", approval_token=token)
    assert config.state()["value"]["maximum"] == original["changes"]["maximum"]
    journal, registry, _, owner, context, _ = runtime
    parameters = {"x": 1, "y": 2}
    original_validate = owner.validate

    def validate_and_mutate(grant, operation, checked):
        original_validate(grant, operation, checked)
        parameters["x"] = 100
        checked["x"] = 100

    owner.validate = validate_and_mutate
    # Preserve the fixture's second owner-side check while observing the exact
    # command forwarded after the admission callback mutates its own copy.
    def move(context, dispatched):
        assert dispatched == {"x": 1, "y": 2}
        owner.position.update(dispatched)
        return {"position": dict(dispatched), "objective_achieved": True, "quality": "not_configured"}

    owner.move = move
    result = Executor(registry, journal, owner).execute(context, "fixture.move", "1", parameters)
    assert result["state"] == "succeeded"
    assert result["request"]["parameters"] == {"x": 1, "y": 2}


def test_configuration_retains_exact_approval_proof_and_readback_after_publish(config):
    candidate = proposal(config, {"minimum": -5, "maximum": 5})
    config.adopt(candidate, Adoption(), boundary="idle", approval_token=approve(config, candidate))
    events = config.history(limit=2)
    events += config.history(after=events[-1]["sequence"], limit=2)
    assert [event["kind"] for event in events] == ["approval", "prepare_intent", "adoption_phase", "effective"]
    assert events[0]["body"]["proposal"] == candidate
    assert events[-1]["body"]["pending"]["proof"]["current_and_future_trajectories_checked"]
    assert events[-1]["body"]["evidence"]["effective_revision"] == candidate["revision"]
    assert config.state()["pending"] is None


def test_cooperative_cancel_after_output_does_not_become_prewrite_refusal(runtime):
    journal, registry, _, owner, context, _ = runtime

    def move_then_cancel(context, parameters):
        owner.position.update(parameters)
        owner.writes += 1
        context.cancelled = True
        context.check()

    owner.move = move_then_cancel
    result = Executor(registry, journal, owner).execute(context, "fixture.move", "1", {"x": 1, "y": 2})
    assert result["state"] == "uncertain"
    assert result["result"]["error"]["code"] == "cancelled"
    assert result["result"]["effect"] == "unknown"
