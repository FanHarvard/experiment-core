"""Domain-neutral gateway/task relationships and result authorship.

Roles select responsibilities, not model price, provider, intelligence or physical
authority. A gateway is a communication parent; its liveness is not a task lease.
Domain hosts own permissions, evidence evaluation and effectful adoption.
"""
import copy
import math

from experiment_core.agent_contracts import execution_profile, text
from experiment_core.records import safe_id

VERSION = 'experiment.agent-roles/1'
ROLES = frozenset(('gateway', 'task'))
ADOPTION_MODES = frozenset(('report_only', 'human_confirmation', 'auto_within_approved'))
RESPONSIBILITIES = {
    'gateway': ('receive_requests', 'dispatch_cards', 'inspect_progress', 'request_cancellation', 'relay_authored_results'),
    'task': ('evaluate_evidence', 'propose_next_step', 'evaluate_outcome', 'author_explanation'),
}
ROLE_INSTRUCTIONS = {
    'gateway': '''You are the conversation and card-dispatch gateway.
Handle requests to start, cancel or inspect registered operations, manage task
requests, and relay task-authored results with their author attribution.
Delegate domain analysis, optimization and explanations to the card task agent.
Unless the human explicitly selects another execution profile, omit the profile
override: inherit this gateway interface and use the card model/effort mapping.
Do not copy a provider or model from conversation history as a new override.
Do not replace its judgment or silently rewrite its final explanation. If an
explanation is unavailable, report that fact and preserve the recorded outcome.
A gateway disconnect does not cancel, replay or transfer an admitted task.
Model selection changes computation, never permissions or physical authority.''',
    'task': '''You are an ordinary card task agent, including when the card is
monitoring, diagnosis or adjustment. Evaluate the supplied evidence, propose the
next permitted step, evaluate the outcome and author the final explanation from
host-confirmed results. The gateway is a communication endpoint, not an extra
scientific decision maker. Preserve the task's budget and authority boundaries;
do not infer approval from your model, role, parent or another agent's message.''',
}


def role_contract(role):
    """Responsibilities are independent of provider, cost, effort and lifetime."""
    if not isinstance(role, str) or role not in ROLES:
        raise ValueError('Unknown agent role')
    return dict(schema=VERSION, role=role, responsibilities=list(RESPONSIBILITIES[role]),
                instructions=ROLE_INSTRUCTIONS[role])


def assignment(role, profile):
    """Pin explicit computation settings without imposing a role-specific model."""
    return dict(**role_contract(role), profile=profile_snapshot(profile))


def profile_snapshot(profile):
    """Normalize computation plus optional registry provenance, without lookup."""
    if not isinstance(profile, dict):
        raise ValueError('Expected execution profile')
    provenance = {k: profile[k] for k in ('id', 'registry_revision', 'profile_revision') if k in profile}
    for key, value in provenance.items():
        if key == 'id':
            safe_id(value)
        else:
            text(value, key, 160)
    computation = execution_profile({k: v for k, v in profile.items() if k not in provenance})
    return {**provenance, **computation}


def relationship(task_id, *, gateway_id=None, parent_session=None, profile=None,
                 purpose='card_execution'):
    safe_id(task_id)
    if gateway_id is not None:
        safe_id(gateway_id)
        if gateway_id == task_id:
            raise ValueError('A task cannot be its own gateway')
    # Purpose is a registered host label, not another privileged agent role.
    safe_id(purpose)
    if parent_session is not None:
        text(parent_session, 'parent session', 512)
    if profile is not None:
        profile = profile_snapshot(profile)
    return dict(schema=VERSION, role='task', task_id=task_id, gateway_id=gateway_id,
        parent_session=parent_session, purpose=purpose, profile=copy.deepcopy(profile),
        gateway_liveness_required=False, decision_source_count=1, delegation_depth=1,
        explanation_author='task', physical_authority='domain_host_grant_only')


def adoption_mode(value):
    if not isinstance(value, str) or value not in ADOPTION_MODES:
        raise ValueError('Unknown adoption mode')
    return value


def adoption_disposition(mode, *, verified, within_approved_scope, requires_human=False):
    """Resolve eligibility; this grants no authority and performs no effects."""
    adoption_mode(mode)
    if any(type(v) is not bool for v in (verified, within_approved_scope, requires_human)):
        raise ValueError('Adoption evidence must use explicit host booleans')
    if not verified:
        return 'unverified'
    if mode == 'report_only':
        return 'proposal_only'
    if mode == 'human_confirmation' or requires_human or not within_approved_scope:
        return 'awaiting_human'
    return 'eligible_for_host_adoption'


def explanation(text, *, task_id, session, profile, model_info):
    safe_id(task_id)
    if not isinstance(text, str) or not text.strip():
        raise ValueError('Task explanation must be nonempty text')
    profile = profile_snapshot(profile)
    if not isinstance(session, str) or not session:
        raise ValueError('Task explanation requires its author session')
    if not isinstance(model_info, dict):
        raise ValueError('Task explanation requires normalized adapter attestation')
    if (model_info.get('confirmation') != 'server_reported' or
            model_info.get('resolved_model') != profile.get('model') or
            (profile.get('effort') and model_info.get('resolved_effort') != profile['effort'])):
        raise ValueError('Explanation author profile is unconfirmed')
    return dict(schema=VERSION, status='completed', text=text, task_id=task_id,
        author=dict(role='task', profile=copy.deepcopy(profile), session=session,
                    model_info=copy.deepcopy(model_info)), relay='verbatim')


def explanation_unavailable(error):
    return dict(schema=VERSION, status='unavailable', error_type=type(error).__name__,
        reason=str(error), relay='no_gateway_substitution')


def decision_deadline(task_deadline, decision_deadline):
    """Delegation never extends a card's admitted computation deadline."""
    if any(type(v) not in (int, float) or not math.isfinite(v)
           for v in (task_deadline, decision_deadline)):
        raise ValueError('Decision deadlines must be finite monotonic times')
    return min(task_deadline, decision_deadline)
