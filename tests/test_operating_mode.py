import copy

import pytest

from experiment_core.errors import AuthorizationRequired, Conflict, StaleContext
from experiment_core.operating_mode import OperatingModeAuthority, OperatingModeAuthorization, validate_change_request
from experiment_core.records import Journal, digest


@pytest.fixture
def mode(tmp_path):
    journal = Journal(tmp_path / 'owner.sqlite3')
    clock = [1000.]
    authority = OperatingModeAuthority(journal, 'laser', clock=lambda: clock[0])
    yield authority, clock, journal
    journal.close()


def request(**updates):
    return dict(dict(schema_version=1, laser_id='laser', request_id='change', expected_revision=0,
        desired_mode='characterization', authenticated_operator='operator', reason='Bounded investigation',
        issued_unix_s=1000.), **updates)


def authorize(value):
    return OperatingModeAuthorization(digest(value), value['authenticated_operator'])


def test_mode_is_durable_independent_intent_with_exact_cas(mode):
    authority, clock, journal = mode
    original = authority.status()
    assert original['mode'] == 'measurement' and original['revision'] == 0
    assert not original['behavior']['model_can_change_mode']
    value = request()
    result = authority.change(value, authorize(value))
    assert result['changed'] and result['operating_mode']['revision'] == 1
    reloaded = OperatingModeAuthority(journal, 'laser', clock=lambda: clock[0])
    assert reloaded.status() == authority.status()
    assert reloaded.status()['behavior']['exploratory_tests']
    with pytest.raises(StaleContext):
        reloaded.require_revision(0)
    with pytest.raises(StaleContext):
        reloaded.require_revision(None)
    with pytest.raises(StaleContext):
        reloaded.require_revision(True)
    assert reloaded.require_revision(1)['mode'] == 'characterization'
    other = OperatingModeAuthority(journal, 'other', clock=lambda: clock[0])
    assert other.status()['mode'] == 'measurement' and other.status()['revision'] == 0


@pytest.mark.parametrize('authorization', [None, True, {'authenticated_operator': 'operator'}])
def test_received_claims_cannot_authorize_a_mode_change(mode, authorization):
    authority, _, _ = mode
    with pytest.raises(AuthorizationRequired):
        authority.change(request(), authorization)
    assert authority.status()['revision'] == 0


def test_authorization_is_bound_to_exact_scope_and_values(mode):
    authority, _, _ = mode
    value = request()
    for changed in (request(reason='different'), request(laser_id='other')):
        with pytest.raises(AuthorizationRequired):
            authority.change(changed, authorize(value))
    stale = request(expected_revision=1)
    with pytest.raises(StaleContext):
        authority.change(stale, authorize(stale))
    assert authority.status()['revision'] == 0


def test_replay_never_reverts_a_later_mode_or_renews_authority(mode):
    authority, clock, _ = mode
    first = request()
    authority.change(first, authorize(first))
    second = request(request_id='back', expected_revision=1, desired_mode='measurement')
    authority.change(second, authorize(second))
    clock[0] += 10000
    replay = authority.change(first, authorize(first))
    assert replay['idempotent_replay'] and replay['operating_mode']['mode'] == 'characterization'
    assert authority.status()['mode'] == 'measurement' and authority.status()['revision'] == 2
    changed = request(reason='changed replay')
    with pytest.raises(Conflict):
        authority.change(changed, authorize(changed))


@pytest.mark.parametrize('issued', [699., 1006., float('inf')])
def test_new_request_has_bounded_authorization_lifetime(mode, issued):
    authority, _, _ = mode
    value = request(issued_unix_s=issued)
    with pytest.raises((AuthorizationRequired, ValueError)):
        authority.change(value, authorize(value))
    assert authority.status()['revision'] == 0


def test_same_mode_is_a_recorded_noop(mode):
    authority, _, _ = mode
    value = request(desired_mode='measurement')
    result = authority.change(value, authorize(value))
    assert not result['changed'] and authority.status()['revision'] == 0
    assert authority.change(value, authorize(value))['idempotent_replay']


@pytest.mark.parametrize('patch', [dict(desired_mode='unsafe'), dict(safety={'upper': 1000}),
    dict(expected_revision=True), dict(reason=''), dict(schema_version=True)])
def test_contract_cannot_carry_safety_overrides_or_ambiguous_values(patch):
    value = copy.deepcopy(request())
    value.update(patch)
    with pytest.raises(ValueError):
        validate_change_request(value)
