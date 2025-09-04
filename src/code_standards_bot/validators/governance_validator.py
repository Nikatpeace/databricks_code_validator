"""Governance validator for checking governance-related rules."""

import re
from typing import List

from .base_validator import BaseValidator
from ..models.validation_result import ValidationResult
from ..models.notebook_metadata import NotebookMetadata
from ..services.llm_service import LLMService
from ..config.yaml_config import YamlConfigManager
from ..utils.logging_utils import get_logger

logger = get_logger(__name__)

class GovernanceValidator(BaseValidator):
    """Validator for governance rules."""
    
    def __init__(self, llm_service: LLMService, config_manager: YamlConfigManager = None):
        """Initialize the governance validator."""
        super().__init__("Governance Validator")
        self.llm_service = llm_service
        self.config_manager = config_manager or YamlConfigManager()
    
    def validate(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """
        Validate governance-related rules.
        
        Args:
            notebook: The notebook metadata to validate, containing code cells and path
            
        Returns:
            List of validation results
        """
        results = []
        config = self.config_manager.get_config()
        
        if config.governance_validation.enabled:
            logger.debug(f"Validating governance rules for {notebook.path}")
            
            if self.config_manager.is_rule_enabled("governance_validation", "unity_catalog_usage"):
                results.append(self._validate_unity_catalog_usage(notebook))
            
            if self.config_manager.is_rule_enabled("governance_validation", "documentation_and_comments"):
                results.append(self._validate_documentation_and_comments(notebook))
            
            if self.config_manager.is_rule_enabled("governance_validation", "version_control_integration"):
                results.append(self._validate_version_control_integration(notebook))
            
            if self.config_manager.is_rule_enabled("governance_validation", "data_isolation"):
                results.append(self._validate_data_isolation(notebook))
            
            if self.config_manager.is_rule_enabled("governance_validation", "metadata_tagging"):
                results.extend(self._validate_metadata_tagging(notebook))
        
        return results
    
    def _validate_unity_catalog_usage(self, notebook: NotebookMetadata) -> ValidationResult:
        """Validate use of Unity Catalog."""
        all_code = notebook.get_all_code()
        
        has_disallowed = bool(re.search(r'\b(hive_metastore|dbfs)\b', all_code, re.IGNORECASE))
        
        if has_disallowed:
            return self.create_failed_result(
                "Unity Catalog Usage",
                "Disallowed metastore (hive_metastore or dbfs) found; use Unity Catalog",
                notebook.path
            )
        else:
            return self.create_passed_result(
                "Unity Catalog Usage",
                "Unity Catalog usage validated",
                notebook.path
            )
    
    def _validate_documentation_and_comments(self, notebook: NotebookMetadata) -> ValidationResult:
        """Validate documentation and comments using simple pattern matching."""
        all_code = notebook.get_all_code()
        
        try:
            # Get comment patterns from config
            params = self.config_manager.get_rule_parameters("governance_validation", "documentation_and_comments")
            comment_patterns = params.get("check_patterns", ["#", "//", "/*", '"""'])
            min_ratio = params.get("min_comment_ratio", 0.1)
            
            # Count total lines and comment lines
            lines = all_code.split('\n')
            total_lines = len([line for line in lines if line.strip()])  # Non-empty lines
            comment_lines = 0
            
            for line in lines:
                line_stripped = line.strip()
                if any(line_stripped.startswith(pattern) for pattern in comment_patterns):
                    comment_lines += 1
            
            if total_lines == 0:
                return self.create_passed_result(
                    "Documentation and Comments",
                    "No code to validate",
                    notebook.path
                )
            
            comment_ratio = comment_lines / total_lines
            
            if comment_ratio >= min_ratio:
                return self.create_passed_result(
                    "Documentation and Comments",
                    f"Sufficient comments found ({comment_ratio:.1%} of lines)",
                    notebook.path
                )
            else:
                return self.create_failed_result(
                    "Documentation and Comments",
                    f"Insufficient comments ({comment_ratio:.1%} of lines, minimum {min_ratio:.1%} required)",
                    notebook.path
                )
        except Exception as e:
            logger.error(f"Error validating documentation: {e}")
            return self.create_failed_result(
                "Documentation and Comments",
                "Error checking comments; ensure code has adequate documentation",
                notebook.path
            )
    
    def _validate_version_control_integration(self, notebook: NotebookMetadata) -> ValidationResult:
        """Validate version control integration."""
        all_code = notebook.get_all_code()
        
        has_git = bool(re.search(r'%run.*repos|dbutils\.notebook\.run.*repos', all_code, re.IGNORECASE))
        
        if has_git:
            return self.create_passed_result(
                "Version Control Integration",
                "Version control integration (repos) found",
                notebook.path
            )
        else:
            return self.create_failed_result(
                "Version Control Integration",
                "No version control integration detected; use Databricks Repos",
                notebook.path
            )
    
    def _validate_data_isolation(self, notebook: NotebookMetadata) -> ValidationResult:
        """Validate data isolation with environment-specific paths."""
        all_code = notebook.get_all_code()
        
        has_isolated = bool(re.search(r'dev/|staging/|prod/', all_code, re.IGNORECASE))
        has_mixed = bool(re.search(r'(dev/.*staging|staging/.*prod|dev/.*prod)', all_code, re.IGNORECASE))
        
        if has_isolated and not has_mixed:
            return self.create_passed_result(
                "Data Isolation",
                "Data paths are isolated by environment",
                notebook.path
            )
        else:
            return self.create_failed_result(
                "Data Isolation",
                "Mixed or non-isolated data paths detected; use dev/, staging/, or prod/ prefixes",
                notebook.path
            )
    
    def _validate_metadata_tagging(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate metadata tagging requirements for tables and datasets."""
        results = []
        all_code = notebook.get_all_code()
        
        # Find CREATE TABLE statements
        table_matches = re.finditer(
            r'CREATE TABLE\s+(?:[a-z0-9_]+\.)?([a-z0-9_]+)', 
            all_code, 
            re.IGNORECASE
        )
        
        required_tags = self.config_manager.get_rule_parameters("governance_validation", "metadata_tagging").get(
            "required_tags", ["OWNER", "CLASSIFICATION", "PURPOSE"]
        )
        
        for match in table_matches:
            table_name = match.group(1)
            table_section = match.group(0)
            
            missing_tags = []
            for tag in required_tags:
                # Check for TBLPROPERTIES with required tags
                if not re.search(rf'TBLPROPERTIES.*[\'"]?{tag.lower()}[\'"]?\s*=', table_section, re.IGNORECASE | re.DOTALL):
                    missing_tags.append(tag)
            
            if missing_tags:
                results.append(self.create_failed_result(
                    "Metadata Tagging",
                    f"Table '{table_name}' missing required metadata tags: {', '.join(missing_tags)}",
                    notebook.path
                ))
            else:
                results.append(self.create_passed_result(
                    "Metadata Tagging",
                    f"Table '{table_name}' has all required metadata tags",
                    notebook.path
                ))
        
        # If no tables found, check for dataset/view metadata in comments
        if not results:
            has_metadata_comments = bool(re.search(
                r'#.*(?:owner|classification|purpose|business owner)', 
                all_code, 
                re.IGNORECASE
            ))
            
            if has_metadata_comments:
                results.append(self.create_passed_result(
                    "Metadata Tagging",
                    "Metadata information found in comments",
                    notebook.path
                ))
            else:
                results.append(self.create_failed_result(
                    "Metadata Tagging",
                    "No metadata tags or ownership information found; add TBLPROPERTIES or comments",
                    notebook.path
                ))
        
        return results