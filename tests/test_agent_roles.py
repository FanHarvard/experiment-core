import pytest
from experiment_core.agent_roles import (relationship, explanation, explanation_unavailable,
    adoption_disposition, decision_deadline, assignment, role_contract)


def test_monitor_task_has_same_role_and_no_gateway_liveness_dependency():
    r=relationship('task',gateway_id=None,profile={'provider':'fixture','model':'any'},purpose='monitor_review')
    assert r['role']=='task' and r['gateway_liveness_required'] is False
    assert r['physical_authority']=='domain_host_grant_only'
    with pytest.raises(ValueError):relationship('same',gateway_id='same')


def test_adoption_does_not_invent_approval():
    assert adoption_disposition('auto_within_approved',verified=True,within_approved_scope=True)=='eligible_for_host_adoption'
    assert adoption_disposition('auto_within_approved',verified=True,within_approved_scope=False)=='awaiting_human'
    assert adoption_disposition('auto_within_approved',verified=False,within_approved_scope=True)=='unverified'
    assert adoption_disposition('report_only',verified=True,within_approved_scope=True)=='proposal_only'


def test_explanation_keeps_exact_author_profile_and_cannot_silently_fallback():
    profile={'provider':'fixture','model':'model-a','effort':'high'}
    info={'resolved_model':'model-a','resolved_effort':'high','confirmation':'server_reported'}
    r=explanation('Evidence and outcome',task_id='task',session='session',profile=profile,model_info=info)
    assert r['relay']=='verbatim' and r['author']['role']=='task'
    with pytest.raises(ValueError):explanation('Fallback',task_id='task',session='other',profile=profile,model_info={**info,'resolved_effort':'low'})
    assert explanation_unavailable(RuntimeError('failed'))['relay']=='no_gateway_substitution'
    assert decision_deadline(100,30)==30


def test_role_does_not_force_gateway_cost_or_task_effort():
    # Deliberately invert the usual cheap gateway / stronger task preference.
    gateway = assignment('gateway', {'provider':'fixture', 'model':'large', 'effort':'max'})
    task = assignment('task', {'provider':'another', 'model':'small', 'effort':'low'})
    assert gateway['profile']['model']=='large' and gateway['profile']['effort']=='max'
    assert task['profile']['model']=='small' and task['profile']['effort']=='low'
    assert 'evaluate_evidence' not in gateway['responsibilities']
    assert 'author_explanation' in task['responsibilities']
    gateway['responsibilities'].append('invented')
    assert 'invented' not in role_contract('gateway')['responsibilities']


def test_non_laser_monitor_uses_ordinary_task_contract_and_pinned_profile():
    profile={'provider':'fixture','model':'inspection','effort':'medium'}
    r=relationship('surface-review',gateway_id='operator-console',parent_session='conversation',
        profile=profile,purpose='surface_uniformity_review')
    profile['model']='changed'
    assert r['profile']['model']=='inspection'
    assert r['role']=='task' and not r['gateway_liveness_required']
    assert r['explanation_author']=='task' and r['decision_source_count']==1


@pytest.mark.parametrize('value', ['false', 1, None])
def test_adoption_never_interprets_truthy_claims_as_host_evidence(value):
    with pytest.raises(ValueError):
        adoption_disposition('auto_within_approved',verified=value,within_approved_scope=True)


@pytest.mark.parametrize('value', [float('inf'), float('nan'), True, '100'])
def test_invalid_deadline_cannot_extend_budget(value):
    with pytest.raises(ValueError):decision_deadline(100,value)


def test_unsupported_role_and_missing_computation_configuration_fail_explicitly():
    with pytest.raises(ValueError):role_contract('privileged_monitor')
    with pytest.raises(ValueError):assignment('gateway', {'effort':'low'})
