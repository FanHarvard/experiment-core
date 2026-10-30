"""Domain-neutral contracts and an explicit local reference runtime.

Runtime components are imported from their documented modules. Importing this
package does not create stores, register applications or contact any service.
"""
from experiment_core.validation import ContractError, schema, validate_message

__all__ = ["ContractError", "schema", "validate_message"]
