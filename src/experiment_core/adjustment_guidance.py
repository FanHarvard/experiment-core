"""Advisory proposal sizes; never physical limits or execution authority."""
from copy import deepcopy
import math

from experiment_core.records import digest, safe_id


def validate(value):
    if (not isinstance(value, dict) or set(value) != {'schema_version', 'knobs'}
            or type(value['schema_version']) is not int or value['schema_version'] != 1
            or not isinstance(value['knobs'], dict) or len(value['knobs']) > 128):
        raise ValueError('Expected versioned per-knob adjustment guidance')
    for name, item in value['knobs'].items():
        safe_id(name)
        if not isinstance(item, dict) or set(item) != {'kind', 'step', 'unit', 'reason'}:
            raise ValueError('Guidance requires kind, step, unit and reason')
        if item['kind'] not in ('absolute', 'relative'):
            raise ValueError('Guidance kind must be absolute or relative')
        if type(item['step']) not in (int, float) or not math.isfinite(item['step']) or item['step'] <= 0:
            raise ValueError('Suggested step must be positive and finite')
        for key, maximum in (('unit', 64), ('reason', 1024)):
            text = item[key]
            if (not isinstance(text, str) or not text.strip() or len(text.encode()) > maximum
                    or any(ord(c) < 32 or ord(c) == 127 for c in text)):
                raise ValueError('Invalid guidance '+key)
        if item['kind'] == 'relative' and item['unit'] != 'fraction':
            raise ValueError('Relative advice uses a dimensionless fraction of the current magnitude')
    return deepcopy(value)


def snapshot(value):
    value = validate(value)
    return dict(document=value, revision=digest(value), advisory_only=True,
                interpretation='An initial proposal size, not a minimum, maximum, safety limit or permission')


def instructions():
    return ('Per-knob adjustment_guidance is advisory, not an execution bound. Start a local comparison '
            'near the suggested initial step when it is appropriate to the current evidence. An absolute '
            'step uses the stated physical unit; a relative step is a fraction of the magnitude of the '
            'current parameter, not of its allowed range. Relative advice at zero does not define a step. '
            'Use previous comparable responses, sensitivity, resolution, drift and recovery cost to choose '
            'a smaller or larger step, and explain deviations in the decision reason. A permitted excursion '
            'is not a recommended trial step: never select half the allowed span merely because it is allowed. '
            'Characterization mode does not remove this distinction. Enlarge only when observations support '
            'it; reduce after a large response or loss of the branch. Missing guidance means unknown, not '
            'permission to use the range endpoint. Read current adjustment guidance before planning a new knob '
            'trial if no current guidance snapshot was supplied. Advice alone never changes output, supported '
            'capabilities, approved envelopes, physical limits, freshness requirements or foreground priority.')
