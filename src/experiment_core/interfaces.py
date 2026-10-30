"""Integration ports, not implementations or security boundaries.

Only the domain owner validates and performs actions. Decision providers propose
messages and never acquire instrument transports. Registries and authentication
are supplied by trusted host integration, not dynamically by received messages.
"""
from typing import Any, Mapping, Protocol

Message = Mapping[str, Any]


class DecisionProvider(Protocol):
    """Deterministic algorithm, resident model or human step interface."""

    def propose(self, measurement: Message, capabilities: Message,
                context: Message) -> Message: ...


class SystemOwner(Protocol):
    """Application implements physical meaning, policy and execution state."""

    def describe(self, subject_id: str) -> Message: ...
    def preflight(self, proposal: Message, trusted_context: object) -> Message: ...
    def submit(self, proposal: Message, trusted_context: object) -> Message: ...
    def status(self, subject_id: str, run_id: str, request_id: str) -> Message: ...
    def cancel(self, subject_id: str, run_id: str, request_id: str,
               trusted_context: object) -> Message: ...


# Historical 0.1 protocol name, retained for callers and stored message readers.
DomainOwner = SystemOwner


class InstrumentOwner(Protocol):
    """Single physical writer; authenticates the host-supplied scoped context."""

    def describe(self) -> Message: ...
    def validate(self, context: object, operation: object, parameters: Message) -> None: ...
    def validate_cancel(self, context: object, operation: object) -> None: ...
    def inspect(self, context: object) -> Message: ...
    def quarantine(self, context: object) -> None: ...


class EventStore(Protocol):
    """Host-side append/CAS and durable reconciliation; no replay by default."""

    def append(self, run_id: str, expected_sequence: int, event: Message) -> int: ...
    def read(self, run_id: str, after_sequence: int) -> list[Message]: ...


class ArtifactStore(Protocol):
    """Scoped IDs resolve locally; messages do not carry secret download URLs."""

    def describe(self, subject_id: str, artifact_id: str) -> Message: ...
