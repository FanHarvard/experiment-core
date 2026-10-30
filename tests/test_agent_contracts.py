"""Computation selection cannot create authority or silently drop evidence."""
import hashlib
import pytest
from experiment_core.agent_contracts import execution_profile, execution_policy, choose_profile, instruction_bundle
from experiment_core.records import canonical_json, digest


def test_required_specialist_is_selected_even_without_resident_request():
    p=execution_policy(dict(mode='required',task_profile='deep',escalation_profiles=['deeper']))
    assert choose_profile(p,requested=False)=='deep'
    assert choose_profile(p,override='deeper')=='deeper'
    with pytest.raises(ValueError):choose_profile(p,override='cheap-unapproved')
    with pytest.raises(ValueError):execution_policy(dict(mode='required'))


def test_resident_and_optional_policy_do_not_silently_gain_specialists():
    assert choose_profile(None) is None
    with pytest.raises(ValueError):choose_profile(dict(mode='resident'),override='deep')
    policy=dict(mode='on_request',task_profile='deep')
    assert choose_profile(policy,requested=False) is None
    assert choose_profile(policy,requested=True)=='deep'


def test_exact_opaque_route_round_trips_without_aliasing_or_shared_mutation():
    value=dict(provider='deepseek_harness',model='["provider","exact-model"]',required_tools=['read'])
    result=execution_profile(value)
    assert result['model']==value['model'] and result['effort']==''
    result['required_tools'].append('write')
    assert value['required_tools']==['read']
    with pytest.raises(ValueError):execution_profile(dict(value,model=''))


@pytest.mark.parametrize('patch',[dict(required_tools=[{}]),dict(required_modalities=[[]]),dict(max_turns=True),
    dict(timeout_s=float('nan')),dict(effort='high\x7f'),dict(safety_bypass=True)])
def test_invalid_profile_reports_domain_error(patch):
    with pytest.raises(ValueError):execution_profile(dict(provider='codex',model='exact',**patch))


def test_bundle_hashes_utf8_bytes_and_never_truncates_over_budget():
    context={'laser_id':'laser','label':'日本語'};evidence={'samples':[1,2]}
    result=instruction_bundle(b'common',b'task',revision='1',context=context,evidence=evidence)
    assert result['instructions']=='common\n\ntask'
    assert result['instructions_sha256']==hashlib.sha256(b'common\n\ntask').hexdigest()
    assert result['input_bytes']==len(canonical_json(result['input']).encode())
    assert result['input_sha256']==digest(result['input'])
    context['laser_id']='other';evidence['samples'].append(3)
    assert result['input']['context']['laser_id']=='laser' and result['input']['evidence']['samples']==[1,2]
    with pytest.raises(ValueError,match='exceeds'):
        instruction_bundle(b'common',b'task',revision='1',context=context,evidence=evidence,max_input_bytes=1)
    with pytest.raises(ValueError):
        instruction_bundle(b'common',b'task',revision='1',context={},evidence={'value':float('inf')})


def test_provider_inheritance_is_explicit_and_never_falls_back_to_another_harness():
    policy=dict(mode='required',provider_profiles={'claude':'claude-standard','codex':'codex-standard'},escalation_profiles=['codex-deep'])
    assert choose_profile(policy,provider='claude')=='claude-standard'
    assert choose_profile(policy,provider='claude',override='codex-deep')=='codex-deep'
    with pytest.raises(ValueError,match='gateway provider'):choose_profile(policy,provider='opencode')
    with pytest.raises(ValueError):choose_profile(policy,provider='claude',override='anything')
    with pytest.raises(ValueError):execution_policy({**policy,'task_profile':'fixed'})
