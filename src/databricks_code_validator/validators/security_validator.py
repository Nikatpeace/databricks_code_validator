"""Security validator for checking security-related rules."""

import re
from typing import List

from .base_validator import BaseValidator
from ..models.validation_result import ValidationResult
from ..models.notebook_metadata import NotebookMetadata
from ..services.llm_service import LLMService
from ..config.yaml_config import YamlConfigManager
from ..utils.logging_utils import get_logger

logger = get_logger(__name__)

class SecurityValidator(BaseValidator):
    """Validator for security rules."""
    
    def __init__(self, llm_service: LLMService, config_manager: YamlConfigManager = None):
        """Initialize the security validator."""
        super().__init__("Security Validator")
        self.llm_service = llm_service
        self.config_manager = config_manager or YamlConfigManager()
    
    def validate(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """
        Validate security-related rules.
        
        Args:
            notebook: The notebook metadata to validate, containing code cells and path
            
        Returns:
            List of validation results
        """
        results = []
        config = self.config_manager.get_config()
        
        if config.security_validation.enabled:
            logger.debug(f"Validating security rules for {notebook.path}")
            
            if self.config_manager.is_rule_enabled("security_validation", "no_hard_coded_secrets"):
                results.extend(self._validate_no_hard_coded_secrets(notebook))
            
            if self.config_manager.is_rule_enabled("security_validation", "access_controls"):
                results.extend(self._validate_access_controls(notebook))
            
            if self.config_manager.is_rule_enabled("security_validation", "sql_injection_risks"):
                results.append(self._validate_sql_injection_risks(notebook))
            
        
        return results
    
    def _validate_no_hard_coded_secrets(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate avoidance of hard-coded secrets."""
        results = []
        all_code = notebook.get_all_code()
        
        # Regex patterns from config or default
        patterns = self.config_manager.get_rule_parameters("security_validation", "no_hard_coded_secrets").get(
            "regex_patterns", [r"Bearer\\s+\\w+", r"dapi\\w+", r"password\\s*=\\s*['\"][^'\"]+['\"]"]
        )
        
        for pattern in patterns:
            matches = re.finditer(pattern, all_code, re.IGNORECASE)
            for match in matches:
                results.append(self.create_failed_result(
                    "No Hard-Coded Secrets",
                    f"Potential hard-coded secret found: {match.group(0)}",
                    notebook.path
                ))
        
        if not results:
            try:
                llm_prompt = self.config_manager.get_rule_parameters("security_validation", "no_hard_coded_secrets").get(
                    "llm_prompt", "Scan for potential hard-coded secrets in this code. Return 'True' if secure, 'False' if secrets found, with explanation."
                )
                llm_response = self.llm_service.call_llm(all_code, llm_prompt)
                details = llm_response["choices"][0]["message"]["content"]
                match = re.search(r'(True|False)\s*\((.*)\)', details.strip())
                if match and match.group(1).lower() == "true":
                    results.append(self.create_passed_result(
                        "No Hard-Coded Secrets",
                        match.group(2),
                        notebook.path
                    ))
                else:
                    results.append(self.create_failed_result(
                        "No Hard-Coded Secrets",
                        match.group(2) if match else "Potential secrets detected; use dbutils.secrets.get()",
                        notebook.path
                    ))
            except Exception as e:
                logger.error(f"Error validating secrets: {e}")
                results.append(self.create_failed_result(
                    "No Hard-Coded Secrets",
                    "No hard-coded secrets detected",
                    notebook.path
                ))
        
        return results
    
    def _validate_access_controls(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate proper access controls."""
        results = []
        all_code = notebook.get_all_code()
        
        has_grants = bool(re.search(r'GRANT|REVOKE|USE CATALOG', all_code, re.IGNORECASE))
        
        if has_grants:
            results.append(self.create_passed_result(
                "Access Controls",
                "Access controls (GRANT/REVOKE) found",
                notebook.path
            ))
        else:
            results.append(self.create_failed_result(
                "Access Controls",
                "No explicit access controls found; use GRANT/REVOKE or Unity Catalog",
                notebook.path
            ))
        
        return results
    
    def _validate_sql_injection_risks(self, notebook: NotebookMetadata) -> ValidationResult:
        """Validate SQL injection risks."""
        all_code = notebook.get_all_code()
        
        try:
            is_secure, details = self.llm_service.check_sql_injection(all_code)
            if is_secure:
                return self.create_passed_result(
                    "SQL Injection Risks",
                    details,
                    notebook.path
                )
            else:
                return self.create_failed_result(
                    "SQL Injection Risks",
                    details,
                    notebook.path
                )
        except Exception as e:
            logger.error(f"Error validating SQL injection: {e}")
            return self.create_failed_result(
                "SQL Injection Risks",
                f"Error checking SQL injection: {str(e)}",
                notebook.path
            )
    
