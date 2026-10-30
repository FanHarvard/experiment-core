"""Device-independent change/measurement protocol.

Capabilities are descriptive. A host admission and owner revision check are
required separately; a model proposal never authorizes itself.
"""
from typing import Protocol, Mapping, Callable, Any


class MeasurementAdapter(Protocol):
    def check(self) -> None:
        """Verify current ownership, operating context and foreground priority."""

    def preflight(self, changes: list, mode: str, measurements: Mapping,
                  planned: Mapping | None = None) -> dict:
        """Validate all changes and intermediate states without writing outputs."""

    def apply(self, plan: Mapping, sample: Callable[[], None]) -> dict:
        """Apply once; return verified readback and per-parameter application times.

        Atomic/coordinated requests must not silently become ordered writes.
        Uncertain effects require inspection, never automatic mutation replay.
        """

    def read(self, measurements: Mapping) -> dict:
        """Return independently timestamped selected measurement groups."""
