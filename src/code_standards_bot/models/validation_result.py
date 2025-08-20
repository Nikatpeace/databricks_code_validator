"""Data model for validation results."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from enum import Enum


class ValidationStatus(Enum):
    """Enumeration for validation status."""
    PASSED = "Passed"
    FAILED = "Failed"
    PENDING = "Pending"


@dataclass
class ValidationResult:
    """
    Represents the result of a single validation rule.
    
    Attributes:
        rule: The name of the validation rule
        status: The validation status (Passed, Failed, Pending)
        details: Additional details about the validation result
        notebook_path: Path to the notebook being validated
        audit_time: Timestamp when the validation was performed
        audit_id: Unique identifier for this audit
    """
    rule: str
    status: ValidationStatus
    details: str
    notebook_path: str
    audit_time: datetime
    audit_id: str
    
    def to_dict(self) -> dict:
        """Convert the validation result to a dictionary."""
        return {
            "rule": self.rule,
            "status": self.status.value,
            "details": self.details,
            "notebook_path": self.notebook_path,
            "audit_time": self.audit_time,
            "audit_id": self.audit_id
        } 