"""Durable experiment intent, separate from physical policy and safety bounds.

The host authenticates an operator before constructing an authorization. This
module never interprets request text, a model role, or a boolean as authority.
"""
from copy import deepcopy
from dataclasses import dataclass
import json
import math
import time

from experiment_core.errors import AuthorizationRequired, Conflict, StaleContext
from experiment_core.records import canonical_json, digest, safe_id

MODES = ('measurement', 'characterization')
REQUEST_MAX_AGE_S = 300.


def behavior(mode):
    if mode not in MODES:
        raise ValueError('Unknown operating mode')
    return dict(
        priority=('continuity and small justified adjustments' if mode == 'measurement'
                  else 'hypothesis-driven characterization and exploratory tests'),
        exploratory_tests=mode == 'characterization',
        hops=('only through an existing authorized exploration and recovery path'
              if mode == 'characterization' else 'avoid deliberate mode loss'),
        safety='All existing physical, freshness, ownership and recovery limits remain unchanged',
        foreground='Explicit foreground target and sweep operations retain their existing authority',
        model_can_change_mode=False)


def validate_change_request(value):
    fields = {'schema_version', 'laser_id', 'request_id', 'expected_revision', 'desired_mode',
              'authenticated_operator', 'reason', 'issued_unix_s'}
    if (not isinstance(value, dict) or set(value) != fields
            or type(value['schema_version']) is not int or value['schema_version'] != 1):
        raise ValueError('An exact operating-mode change request is required')
    safe_id(value['laser_id'])
    safe_id(value['request_id'])
    if type(value['expected_revision']) is not int or value['expected_revision'] < 0:
        raise ValueError('An operating-mode revision is required')
    if value['desired_mode'] not in MODES:
        raise ValueError('Unknown operating mode')
    for key, maximum in (('authenticated_operator', 512), ('reason', 4096)):
        text = value[key]
        if (not isinstance(text, str) or not text.strip() or len(text.encode()) > maximum
                or any(ord(c) < 32 or ord(c) == 127 for c in text)):
            raise ValueError('Invalid operating-mode ' + key)
    if type(value['issued_unix_s']) not in (int, float) or not math.isfinite(value['issued_unix_s']):
        raise ValueError('A finite authorization time is required')
    canonical_json(value)
    return deepcopy(value)


@dataclass(frozen=True)
class OperatingModeAuthorization:
    """Constructed by the trusted host adapter, never from RPC fields alone."""
    request_digest: str
    authenticated_operator: str


class OperatingModeAuthority:
    def __init__(self, journal, laser_id, *, clock=time.time):
        self.journal, self.laser_id, self.clock = journal, safe_id(laser_id), clock
        with journal.transaction() as db:
            db.execute('CREATE TABLE IF NOT EXISTS operating_modes '
                       '(laser TEXT PRIMARY KEY, document TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS operating_mode_requests '
                       '(laser TEXT, request_id TEXT, fingerprint TEXT, result TEXT, '
                       'PRIMARY KEY(laser,request_id))')
            initial = dict(schema_version=1, laser_id=self.laser_id, mode='measurement', revision=0,
                           updated_unix_s=None, authenticated_operator=None, reason='Default measurement mode')
            db.execute('INSERT OR IGNORE INTO operating_modes VALUES (?,?)',
                       (self.laser_id, canonical_json(initial)))

    def status(self):
        with self.journal._mutex:
            row = self.journal._db.execute('SELECT document FROM operating_modes WHERE laser=?',
                                           (self.laser_id,)).fetchone()
        state = json.loads(row[0])
        return dict(state, behavior=behavior(state['mode']))

    def require_revision(self, expected):
        state = self.status()
        if type(expected) is not int or expected != state['revision']:
            raise StaleContext('Operating mode changed or its revision is missing; read current owner mode')
        return state

    def change(self, request, authorization):
        request = validate_change_request(request)
        fingerprint = digest(request)
        if (request['laser_id'] != self.laser_id
                or not isinstance(authorization, OperatingModeAuthorization)
                or authorization.request_digest != fingerprint
                or authorization.authenticated_operator != request['authenticated_operator']):
            raise AuthorizationRequired('An exact operator-authenticated mode change is required')
        with self.journal.transaction() as db:
            old = db.execute('SELECT fingerprint,result FROM operating_mode_requests '
                             'WHERE laser=? AND request_id=?', (self.laser_id, request['request_id'])).fetchone()
            if old:
                if old[0] != fingerprint:
                    raise Conflict('Operating-mode request ID already binds different content')
                return dict(json.loads(old[1]), idempotent_replay=True)
            age = self.clock() - request['issued_unix_s']
            if not -5 <= age <= REQUEST_MAX_AGE_S:
                raise AuthorizationRequired('Operating-mode authorization is expired or future-dated')
            state = json.loads(db.execute('SELECT document FROM operating_modes WHERE laser=?',
                                          (self.laser_id,)).fetchone()[0])
            if request['expected_revision'] != state['revision']:
                raise StaleContext('Operating-mode revision changed before this request')
            changed = request['desired_mode'] != state['mode']
            if changed:
                state.update(mode=request['desired_mode'], revision=state['revision'] + 1,
                             updated_unix_s=self.clock(), authenticated_operator=request['authenticated_operator'],
                             reason=request['reason'])
                db.execute('UPDATE operating_modes SET document=? WHERE laser=?',
                           (canonical_json(state), self.laser_id))
            result = dict(request_id=request['request_id'], changed=changed, idempotent_replay=False,
                          operating_mode=dict(state, behavior=behavior(state['mode'])))
            db.execute('INSERT INTO operating_mode_requests VALUES (?,?,?,?)',
                       (self.laser_id, request['request_id'], fingerprint, canonical_json(result)))
        return result
