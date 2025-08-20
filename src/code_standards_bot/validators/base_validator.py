"""Base validator class for code standards checking."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import List
import uuid

from ..models.validation_result import ValidationResult, ValidationStatus
from ..models.notebook_metadata import NotebookMetadata


class BaseValidator(ABC):
    """Abstract base class for all validators."""
    
    def __init__(self, name: str):
        """
        Initialize the validator.
        
        Args:
            name: Name of the validator
        """
        self.name = name
    
    @abstractmethod
    def validate(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """
        Validate the notebook against specific rules.
        
        Args:
            notebook: The notebook metadata to validate
            
        Returns:
            List of validation results
        """
        pass
    
    def create_result(
        self, 
        rule: str, 
        status: ValidationStatus, 
        details: str, 
        notebook_path: str
    ) -> ValidationResult:
        """
        Create a validation result.
        
        Args:
            rule: The validation rule name
            status: The validation status
            details: Additional details
            notebook_path: Path to the notebook
            
        Returns:
            ValidationResult object
        """
        return ValidationResult(
            rule=rule,
            status=status,
            details=details,
            notebook_path=notebook_path,
            audit_time=datetime.utcnow(),
            audit_id=str(uuid.uuid4())
        )
    
    def create_passed_result(self, rule: str, details: str, notebook_path: str) -> ValidationResult:
        """Create a passed validation result."""
        return self.create_result(rule, ValidationStatus.PASSED, details, notebook_path)
    
    def create_failed_result(self, rule: str, details: str, notebook_path: str) -> ValidationResult:
        """Create a failed validation result."""
        return self.create_result(rule, ValidationStatus.FAILED, details, notebook_path)
    
    def create_pending_result(self, rule: str, details: str, notebook_path: str) -> ValidationResult:
        """Create a pending validation result."""
        return self.create_result(rule, ValidationStatus.PENDING, details, notebook_path) 