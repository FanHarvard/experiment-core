"""Typed operation failures shared by applications and the reference runtime."""


class OperationError(RuntimeError):
    code = "operation_failed"

    def as_record(self):
        return {"code": self.code, "message": str(self)}


class Conflict(OperationError):
    code = "conflict"


class StaleContext(OperationError):
    code = "stale_context"


class AuthorizationRequired(OperationError):
    code = "authorization_required"


class ObservationUnavailable(OperationError):
    code = "observation_unavailable"


class PrewriteRefusal(OperationError):
    """Only an owner that proves no write occurred may raise this error."""
    code = "prewrite_refusal"


class UncertainWrite(OperationError):
    code = "uncertain_write"


class OwnerFault(OperationError):
    code = "owner_fault"


class Deadline(OperationError):
    code = "deadline"


class Cancelled(OperationError):
    """Cooperative stop; it does not establish whether earlier effects occurred."""
    code = "cancelled"

