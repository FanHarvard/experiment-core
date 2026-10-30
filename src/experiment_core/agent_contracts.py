"""Provider-neutral, versioned agent execution and evidence contracts.

Profiles select computation, never physical authority. Bundles retain provenance
without copying an entire conversation or silently truncating required evidence.
"""
import copy
import hashlib
import math

from experiment_core.records import canonical_json, digest, safe_id


def text(value, name, limit=512, *, empty=False):
    if (not isinstance(value, str) or (not value and not empty)
            or len(value.encode()) > limit or any(ord(c) < 32 or ord(c)==127 for c in value)):
        raise ValueError('Invalid ' + name)
    return value


def positive(value, name, maximum):
    if type(value) not in (float, int) or not math.isfinite(value) or not 0 < value <= maximum:
        raise ValueError('Invalid ' + name)
    return value


def execution_profile(value):
    if not isinstance(value, dict):
        raise ValueError('Execution profile must be a mapping')
    defaults = dict(effort='', required_tools=[], required_modalities=['text'],
                    timeout_s=600., max_turns=8, max_input_bytes=32000)
    if set(value) - {'provider', 'model', *defaults} or not {'provider', 'model'} <= value.keys():
        raise ValueError('Profile requires provider/model and supported execution fields')
    p = {**defaults, **copy.deepcopy(value)}
    safe_id(p['provider'])
    text(p['model'], 'exact model identifier')
    text(p['effort'], 'effort', 80, empty=True)
    positive(p['timeout_s'], 'task timeout', 86400)
    for key, maximum in (('max_turns', 100), ('max_input_bytes', 262144)):
        if type(p[key]) is not int or not 1 <= p[key] <= maximum:
            raise ValueError('Invalid ' + key)
    for key in ('required_tools', 'required_modalities'):
        if (not isinstance(p[key], list) or len(p[key]) > 128 or any(not isinstance(v,str) for v in p[key])
                or len(set(p[key])) != len(p[key])):
            raise ValueError('Invalid ' + key)
        for item in p[key]:
            safe_id(item)
    if set(p['required_modalities']) - {'text', 'image'}:
        raise ValueError('Unsupported required modality')
    return p


DIFFICULTIES = ('basic', 'standard', 'deep')


def difficulty_profile(matrix, provider, difficulty):
    """Resolve an explicit routing table. Profile names carry no semantics."""
    if difficulty not in DIFFICULTIES:
        raise ValueError('Unknown task difficulty')
    name = matrix.get(provider, {}).get(difficulty)
    if not name:
        raise ValueError('No profile for provider/difficulty: ' + provider + '/' + difficulty)
    safe_id(name)
    return name


def execution_policy(value=None):
    defaults = dict(mode='resident', task_profile='', escalation_profiles=[], provider_profiles={}, difficulty='')
    if value is None:
        return defaults
    if not isinstance(value, dict) or set(value) - set(defaults):
        raise ValueError('Invalid agent execution policy')
    p = {**defaults, **copy.deepcopy(value)}
    if p['mode'] not in ('resident', 'on_request', 'required'):
        raise ValueError('Invalid agent execution mode')
    if p['task_profile']:
        safe_id(p['task_profile'])
    if not isinstance(p['provider_profiles'], dict) or len(p['provider_profiles']) > 16:
        raise ValueError('Invalid provider profiles')
    for provider, profile in p['provider_profiles'].items():
        safe_id(provider); safe_id(profile)
    if p['difficulty'] not in ('', 'default', *DIFFICULTIES):
        raise ValueError('Invalid card difficulty')
    if sum(bool(p[k]) for k in ('provider_profiles', 'task_profile', 'difficulty')) > 1:
        raise ValueError('Choose difficulty routing, inherited provider profiles or a fixed task profile')
    if p['mode'] == 'required' and not (p['task_profile'] or p['provider_profiles'] or p['difficulty']):
        raise ValueError('Required delegation needs a task profile')
    if not isinstance(p['escalation_profiles'], list) or len(p['escalation_profiles']) > 32:
        raise ValueError('Invalid escalation profiles')
    for item in p['escalation_profiles']:
        safe_id(item)
    return p


def choose_profile(policy, *, requested=False, override='', provider='', matrix=None, default_difficulty='standard'):
    p = execution_policy(policy)
    if p['mode'] == 'resident':
        if override:
            raise ValueError('This card does not allow delegation')
        return None
    if p['mode'] == 'on_request' and not requested:
        return None
    if p['difficulty']:
        matrix = matrix or {}
        if override:
            allowed = {name for levels in matrix.values() for name in levels.values()} | set(p['escalation_profiles'])
            if override not in allowed:
                raise ValueError('Requested profile is not allowed by this card routing table')
            return override
        return difficulty_profile(matrix, provider, default_difficulty if p['difficulty']=='default' else p['difficulty'])
    if not override and p['provider_profiles'] and provider not in p['provider_profiles']:
        raise ValueError('No card task profile for the selected gateway provider: ' + (provider or 'unresolved'))
    chosen = override or p['provider_profiles'].get(provider) or p['task_profile']
    if not chosen or chosen not in [p['task_profile'], *p['provider_profiles'].values(), *p['escalation_profiles']]:
        raise ValueError('Requested profile is not allowed by this card')
    return chosen


def instruction_bundle(common, task, *, revision, context, evidence, max_input_bytes=32000):
    """Fixed UTF-8 policy bytes plus bounded, separately hashed run evidence."""
    instructions = common + b'\n\n' + task
    instructions.decode('utf-8')
    data = dict(context=copy.deepcopy(context), evidence=copy.deepcopy(evidence))
    encoded = canonical_json(data).encode()
    if len(encoded) > max_input_bytes:
        raise ValueError('Task context exceeds its budget; select evidence or use artifact references')
    return dict(schema_version=1, instruction_revision=revision,
                instructions=instructions.decode(), instructions_sha256=hashlib.sha256(instructions).hexdigest(),
                instructions_bytes=len(instructions), input=data, input_sha256=digest(data),
                input_bytes=len(encoded), full_history_included=False)


class Conditions:
    """Persistable change detector: distinct fresh acquisitions, hysteresis and cooldown.

    Missing/invalid data never clears an unresolved degradation or counts as a
    new observation. The caller persists returned state, not a model conversation.
    """
    def __init__(self, rules, state=None):
        self.rules = self.validate(rules)
        self.state = copy.deepcopy(state or {})

    @staticmethod
    def validate(rules):
        if not isinstance(rules, list) or len(rules) > 32:
            raise ValueError('Expected at most 32 monitor conditions')
        ids = set()
        for rule in rules:
            required = {'id', 'metric', 'above', 'clear_below', 'persistence_s', 'cooldown_s', 'max_age_s'}
            if not isinstance(rule, dict) or not required <= set(rule) <= required | {'baseline','repeat_s'}:
                raise ValueError('Invalid monitor condition fields')
            safe_id(rule['id']); text(rule['metric'], 'metric', 160)
            if rule['id'] in ids:
                raise ValueError('Duplicate monitor condition')
            ids.add(rule['id'])
            for name in ('above', 'clear_below', 'persistence_s', 'cooldown_s', 'max_age_s'):
                v = rule[name]
                if type(v) not in (int, float) or not math.isfinite(v):
                    raise ValueError('Monitor thresholds must be finite')
            if (rule['clear_below'] > rule['above'] or rule['persistence_s'] < 0
                    or rule['cooldown_s'] < 0 or rule['max_age_s'] <= 0):
                raise ValueError('Invalid monitor hysteresis/timing')
            if 'repeat_s' in rule and (type(rule['repeat_s']) not in (int,float) or not math.isfinite(rule['repeat_s']) or rule['repeat_s']<=0):
                raise ValueError('Monitor repeat interval must be finite and positive')
            if 'baseline' in rule and (type(rule['baseline']) not in (int, float)
                    or not math.isfinite(rule['baseline']) or rule['baseline'] == 0):
                raise ValueError('Relative-change baseline must be finite and nonzero')
        return copy.deepcopy(rules)

    def observe(self, metrics, now):
        events = []
        for rule in self.rules:
            old = self.state.setdefault(rule['id'], {})
            if old.get('rule') != digest(rule):
                old.clear(); old['rule'] = digest(rule)
            sample = metrics.get(rule['metric'], {})
            value, timestamp = sample.get('value'), sample.get('acquired_unix_s')
            if (sample.get('valid') is not True or type(value) not in (int, float)
                    or type(timestamp) not in (int, float) or not math.isfinite(value)
                    or not math.isfinite(timestamp) or not 0 <= now - timestamp <= rule['max_age_s']):
                old.pop('since', None)
                continue
            if timestamp <= old.get('last_sample', -math.inf):
                continue
            if 'baseline' in rule:
                value = abs(value-rule['baseline'])/abs(rule['baseline'])
            if timestamp - old.get('last_sample', timestamp) > rule['max_age_s']:
                old.pop('since', None)
            old['last_sample'] = timestamp
            if value <= rule['clear_below']:
                old.pop('since', None); old['active'] = False
            elif value > rule['above']:
                old.setdefault('since', timestamp)
                repeated=bool(old.get('active'))
                due=not repeated or ('repeat_s' in rule and now-old.get('last_event',-math.inf)>=rule['repeat_s'])
                if (due and timestamp - old['since'] >= rule['persistence_s']
                        and now - old.get('last_event', -math.inf) >= rule['cooldown_s']):
                    old.update(active=True, last_event=now)
                    events.append(dict(condition=rule['id'], metric=rule['metric'], value=value,
                                       acquired_unix_s=timestamp, rule_revision=digest(rule), repeated=repeated))
        return events
