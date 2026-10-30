"""Offline protocol tests; no runtime, device or service is contacted."""
import ast
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from experiment_core import ContractError, schema, validate_message


PACKAGE = Path(__file__).resolve().parents[1]
EXAMPLES = sorted((PACKAGE / "examples").glob("*.json"))


def example(name="fixture-measurement"):
    return json.loads((PACKAGE / "examples" / (name + ".json")).read_text())


def test_schema_is_valid_and_does_not_reference_remote_schemas():
    document = schema()
    Draft202012Validator.check_schema(document)

    def visit(value):
        if isinstance(value, dict):
            if "$ref" in value:
                assert value["$ref"].startswith("#/")
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(document)


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda path: path.stem)
def test_generic_examples(path):
    validate_message(json.loads(path.read_text()))


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf"), True])
def test_metric_requires_finite_numeric_value(value):
    message = example()
    message["body"]["metrics"][0]["value"] = value
    with pytest.raises(ContractError):
        validate_message(message)


@pytest.mark.parametrize("value", [
    None, "2026-10-23T22:00:00", "not-a-time", "2026-02-30T22:00:00Z",
    "2026-10-23T22:00:00+00:99", "2026-10-23T22:00:00-00:00",
])
def test_valid_measurement_requires_timestamp_with_timezone(value):
    message = example()
    message["body"]["metrics"][0]["source_time"] = value
    with pytest.raises(ContractError):
        validate_message(message)


@pytest.mark.parametrize("value", ["2026-10-23T22:00:00Z", "2026-10-23T22:00:00.123-04:00"])
def test_supported_timestamp_profiles(value):
    message = example()
    message["body"]["metrics"][0]["source_time"] = value
    validate_message(message)


@pytest.mark.parametrize("state", ["missing", "stale", "invalid"])
def test_unavailable_metric_cannot_claim_a_valid_scalar(state):
    message = example()
    metric = message["body"]["metrics"][0]
    metric["state"] = state
    with pytest.raises(ContractError):
        validate_message(message)
    metric["value"] = None
    validate_message(message)


def test_empty_quality_cannot_be_reported_as_good():
    message = example()
    message["body"]["quality"]["state"] = "good"
    with pytest.raises(ContractError):
        validate_message(message)


def test_missing_configured_quality_is_distinct_from_no_evaluator():
    message = example()
    quality = message["body"]["quality"]
    quality["evaluators"] = [{
        "evaluator_id": "fixture.uniformity", "revision": "v1",
        "state": "unavailable", "reason": "No fresh samples",
    }]
    with pytest.raises(ContractError):
        validate_message(message)
    quality["state"] = "unavailable"
    validate_message(message)
    quality["state"] = "good"
    with pytest.raises(ContractError):
        validate_message(message)


@pytest.mark.parametrize("value", [(), {1: "not a JSON key"}, object()])
def test_only_json_data_is_accepted(value):
    message = example("operation-request")
    message["body"]["parameters"] = value
    with pytest.raises(ContractError):
        validate_message(message)


def test_large_data_must_use_artifacts():
    message = example("operation-request")
    message["body"]["parameters"] = {"raw": "x" * (256 * 1024)}
    with pytest.raises(ContractError, match="256 KiB"):
        validate_message(message)


@pytest.mark.parametrize("key,value", [
    ("version", "99.0"), ("kind", "arbitrary_code"),
    ("issued_at", "2026-10-23T22:00:00"), ("kind", "decision"),
    ("human_approved", True),
])
def test_unknown_envelope_and_mismatched_body_are_rejected(key, value):
    message = example()
    message[key] = value
    with pytest.raises(ContractError):
        validate_message(message)


def test_policy_proposal_cannot_supply_its_own_approval():
    message = example("policy-proposal")
    message["body"]["human_approved"] = True
    with pytest.raises(ContractError):
        validate_message(message)


@pytest.mark.parametrize("action", ["finish", "cancel"])
def test_terminal_decision_does_not_carry_hidden_operations(action):
    message = example("fixture-decision")
    message["body"]["action"] = action
    with pytest.raises(ContractError):
        validate_message(message)
    message["body"].update(capability_id=None, parameter_schema_id=None, parameters={})
    validate_message(message)


def test_execution_decision_requires_a_registered_schema_identifier():
    message = example("fixture-decision")
    message["body"]["parameter_schema_id"] = None
    with pytest.raises(ContractError):
        validate_message(message)


def test_schema_validation_does_not_claim_domain_authorization():
    message = example("operation-request")
    # Deliberately outside any meaningful fixture range. The registered domain
    # validator MUST reject this before execution; generic JSON shape cannot.
    message["body"]["parameters"] = {"position_mm": -1e100}
    validate_message(message)


def test_core_does_not_import_the_ldcon_application():
    from importlib.resources import files
    pending = [files("experiment_core")]
    sources = []
    while pending:
        for path in pending.pop().iterdir():
            if path.is_dir() and path.name != "__pycache__":
                pending.append(path)
            elif path.name.endswith(".py"):
                sources.append(path)
    assert sources, "The imported experiment_core package must contain Python sources"
    for path in sources:
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert all(alias.name.split(".")[0] != "ldcon" for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                assert (node.module or "").split(".")[0] != "ldcon"
