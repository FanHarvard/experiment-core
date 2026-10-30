"""Offline shape validation; passing never authorizes a physical action."""
import json
import re
from datetime import datetime
from importlib.resources import files

from jsonschema import Draft202012Validator, FormatChecker

MAX_MESSAGE_BYTES = 256 * 1024
_FORMATS = FormatChecker()
_TIMESTAMP = re.compile(
    r"\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:[0-5]\d(?:\.\d+)?"
    r"(?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)\Z", re.ASCII
)


@_FORMATS.checks('date-time')
def _timestamp(value):
    """Bounded timestamp profile, independent of optional format packages.

    Include seconds and a known UTC offset. Leap seconds and unknown-offset
    -00:00 need explicit source-clock normalization before serialization.
    """
    if not isinstance(value, str):
        return True  # The schema's type constraint handles non-strings.
    if not _TIMESTAMP.fullmatch(value) or value.endswith('-00:00'):
        return False
    try:
        parsed = datetime.fromisoformat(value.upper().replace('Z', '+00:00'))
    except ValueError:
        return False
    return parsed.tzinfo is not None


class ContractError(ValueError):
    """Malformed protocol data, not an owner action or safety decision."""


def schema():
    """Return a fresh local schema; remote schema retrieval is not supported."""
    return json.loads(files('experiment_core').joinpath('schemas/protocol.json').read_text(encoding='utf-8'))


def _json_value(value):
    if type(value) is dict:
        if any(type(k) is not str for k in value):
            raise ContractError('JSON object keys must be strings')
        for item in value.values():
            _json_value(item)
    elif type(value) is list:
        for item in value:
            _json_value(item)
    elif value is not None and type(value) not in (str, int, float, bool):
        raise ContractError('Only JSON values are supported')


def validate_message(message):
    """Check finite bounded JSON and the draft schema, with no external I/O.

    Domain parameter validation, freshness, resource conflicts, exact approval,
    physical envelope proof and revision reconciliation belong to the owner
    integration. A client-supplied status or ID is never trusted authority.
    """
    try:
        _json_value(message)
        raw = json.dumps(message, allow_nan=False, ensure_ascii=False).encode('utf-8')
    except (ValueError, TypeError, RecursionError, UnicodeError) as error:
        raise ContractError('Expected finite JSON data') from error
    if len(raw) > MAX_MESSAGE_BYTES:
        raise ContractError('Protocol message exceeds 256 KiB; use artifact references')
    validator = Draft202012Validator(schema(), format_checker=_FORMATS)
    error = next(validator.iter_errors(message), None)
    if error:
        path = '/'.join(map(str, error.absolute_path)) or '<root>'
        raise ContractError('Invalid protocol structure at ' + path)
