"""Legacy-compatible invocation metadata; never a physical write authority."""
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass


@dataclass(frozen=True)
class InvocationContext:
    job_id: str = ""
    upstream_failures: tuple = ()


_current = ContextVar("experiment_invocation_context", default=InvocationContext())


def current_context():
    value = _current.get()
    return InvocationContext(value.job_id, tuple(deepcopy(value.upstream_failures)))


@contextmanager
def invocation_context(job_id, failures):
    if not isinstance(failures, list) or any(not isinstance(item, dict) for item in failures):
        raise ValueError("Upstream failure context must be a list of records")
    token = _current.set(InvocationContext(job_id, tuple(deepcopy(failures))))
    try:
        yield current_context()
    finally:
        _current.reset(token)
