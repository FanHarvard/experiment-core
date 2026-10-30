"""Owner-serialized background epochs and foreground generation fencing.

This coordinator performs no I/O or device writes. The application owner must
hold its existing dispatch mutex, persist checkpoint/intent records, and prove
restoration before calling ``complete``. A timeout never grants a handoff.
"""
from copy import deepcopy
import math
import uuid

from experiment_core.errors import Conflict, StaleContext


class BackgroundCoordinator:
    def __init__(self, clock):
        self.clock = clock
        self.generation = 0
        self.reservations = set()
        self.epoch = None
        self.last = None

    def status(self):
        return dict(generation=self.generation, reservations=sorted(self.reservations),
                    epoch=deepcopy(self.epoch), last=deepcopy(self.last),
                    foreground_ready=self.epoch is None)

    def reserve(self, identifiers):
        identifiers = set(identifiers)
        if any(not isinstance(value, str) or not value or len(value) > 256 for value in identifiers):
            raise ValueError('Bounded foreground request identities are required')
        if identifiers - self.reservations:
            self.generation += 1
            if self.epoch:
                self.returning('foreground', requested_at=self.clock())
        self.reservations |= identifiers
        return self.status()

    def release(self, identifiers):
        self.reservations -= set(identifiers)
        return self.status()

    def begin(self, expected_generation, context, checkpoint, timeout_s, identity, *, completion_mode="deadline"):
        if completion_mode not in ("deadline", "verified_handover"):
            raise ValueError("Unknown handoff completion mode")
        if completion_mode == "verified_handover" and timeout_s is not None:
            raise ValueError("Verified handover has no optical restoration deadline")
        if completion_mode == 'deadline' and (type(timeout_s) not in (int, float) or not math.isfinite(timeout_s)
                or not 0 < timeout_s <= 600):
            raise ValueError('An explicit handoff deadline within 600 s is required')
        if self.epoch or self.reservations:
            raise Conflict('A foreground reservation or background epoch is active')
        if expected_generation != self.generation:
            raise StaleContext('Foreground generation changed')
        self.epoch = dict(token=uuid.uuid4().hex, generation=self.generation,
                          context=deepcopy(context), checkpoint=deepcopy(checkpoint),
                          identity=deepcopy(identity), timeout_s=timeout_s, completion_mode=completion_mode,
                          state='active', started_at=self.clock())
        return deepcopy(self.epoch)

    def validate(self, token, context, *, restoring=False):
        epoch = self.epoch
        if epoch is None or token != epoch['token']:
            raise StaleContext('Background epoch is absent or replaced')
        if context != epoch['context']:
            self.block('Owner, policy, target or branch changed')
            raise StaleContext('Background checkpoint context changed')
        if epoch['state'] == 'reconciliation_required':
            raise Conflict(epoch['reason'])
        if not restoring and (epoch['state'] != 'active' or epoch['generation'] != self.generation
                              or self.reservations):
            raise StaleContext('Background proposals are fenced by foreground activity')
        if restoring and self.clock() > epoch.get('return_deadline', math.inf):
            self.block('Checkpoint restoration deadline exceeded')
            raise Conflict('Checkpoint restoration deadline exceeded')
        return deepcopy(epoch)

    def returning(self, reason, *, requested_at=None):
        if self.epoch and self.epoch['state'] == 'active':
            now = self.clock()
            self.epoch.update(state='returning', reason=reason, return_started_at=now,
                              requested_at=now if requested_at is None else requested_at)
            if self.epoch.get('completion_mode') != 'verified_handover':
                self.epoch['return_deadline'] = now + self.epoch['timeout_s']
        return self.status()

    def block(self, reason):
        if self.epoch:
            self.epoch.update(state='reconciliation_required', reason=str(reason))
        return self.status()

    def complete(self, token, context, receipt, *, adopted=False):
        epoch = self.validate(token, context, restoring=not adopted)
        if receipt.get('confirmed') is not True or receipt.get('response_ended') is not True:
            raise Conflict('Confirmed readback and response cleanup are required')
        self.last = dict(epoch, state='adopted' if adopted else 'restored',
                         completed_at=self.clock(), receipt=deepcopy(receipt))
        self.epoch = None
        return self.status()
