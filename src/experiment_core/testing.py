"""Non-laser, non-actuating two-axis fixture for public runtime conformance."""
from experiment_core.errors import PrewriteRefusal
from experiment_core.execution import Operation


class SimulatedStage:
    def __init__(self, reservations, revision="fixture-v1"):
        self.reservations = reservations
        self.revision = revision
        self.position = {"x": 0.0, "y": 0.0}
        self.writes = 0
        self.primary_running = True

    def validate(self, grant, operation, parameters):
        self.reservations.validate(grant, policy_revision=self.revision)
        if any(abs(value) > 10 for value in parameters.values()):
            raise PrewriteRefusal("Stage travel envelope exceeded")

    def quarantine(self, grant):
        self.reservations.quarantine(grant)

    def validate_cancel(self, grant, operation):
        self.reservations.validate_cleanup(grant)

    def move(self, context, parameters):
        self.validate(context.grant, None, parameters)
        self.position.update(parameters)
        self.writes += 1
        return {"position": dict(self.position), "objective_achieved": True, "quality": "not_configured"}


def register_stage(registry):
    registry.register(Operation(
        "fixture.move", "1", {
            "type": "object", "properties": {"x": {"type": "number"}, "y": {"type": "number"}},
            "required": ["x", "y"], "additionalProperties": False,
        }, {
            "type": "object", "required": ["position", "objective_achieved", "quality"],
            "properties": {"position": {"type": "object"}, "objective_achieved": {"type": "boolean"},
                           "quality": {"const": "not_configured"}}, "additionalProperties": False,
        }, frozenset({"stage/x", "stage/y"}),
        lambda owner, context, parameters: owner.move(context, parameters),
        lambda owner, context: {"confirmed": True, "retained": dict(owner.position),
                                "parent_running": owner.primary_running},
        frozenset({"fixture.primary"})))
