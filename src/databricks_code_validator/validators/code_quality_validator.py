"""Comprehensive code quality validator for all code quality-related rules."""

import re
from typing import List
from collections import Counter

from .base_validator import BaseValidator
from ..models.validation_result import ValidationResult
from ..models.notebook_metadata import NotebookMetadata
from ..services.llm_service import LLMService
from ..services.notebook_service import NotebookService
from ..config.yaml_config import YamlConfigManager
from ..utils.logging_utils import get_logger

logger = get_logger(__name__)

class CodeQualityValidator(BaseValidator):
    """Comprehensive validator for all code quality-related rules.
    
    Consolidates validation for:
    - Language standards and consistency
    - Structure and formatting rules  
    - Naming conventions for tables and columns
    - Database design standards
    - Code quality best practices
    """
    
    def __init__(self, llm_service: LLMService, notebook_service: NotebookService, config_manager: YamlConfigManager = None):
        """Initialize the comprehensive code quality validator."""
        super().__init__("Code Quality Validator")
        self.llm_service = llm_service
        self.notebook_service = notebook_service
        self.config_manager = config_manager or YamlConfigManager()
        self.valid_languages = {"sql", "python"}  # Default; override from config
    
    def validate(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """
        Validate comprehensive code quality rules across all categories.
        
        Args:
            notebook: The notebook metadata to validate
            
        Returns:
            List of validation results
        """
        results = []
        config = self.config_manager.get_config()
        
        # Load valid languages from config if available
        if hasattr(config, 'code_quality_validation') and hasattr(config.code_quality_validation, 'rules'):
            valid_langs_rule = config.code_quality_validation.rules.get('valid_languages_only')
            if valid_langs_rule:
                self.valid_languages = valid_langs_rule.parameters.get("allowed_languages", self.valid_languages)
        
        if config.code_quality_validation.enabled:
            logger.debug(f"Validating comprehensive code quality for {notebook.path}")
            
            # Language Standards
            if self.config_manager.is_rule_enabled("code_quality_validation", "valid_languages_only"):
                results.append(self._validate_valid_languages(notebook))
            
            # Structure & Formatting
            if self.config_manager.is_rule_enabled("code_quality_validation", "header_section_exists"):
                results.append(self._validate_header_section(notebook))
            
            
            if self.config_manager.is_rule_enabled("code_quality_validation", "descriptive_cell_titles"):
                results.extend(self._validate_command_cell_titles(notebook))
            
            
            # Database Standards
            if self.config_manager.is_rule_enabled("code_quality_validation", "string_data_types"):
                results.extend(self._validate_character_columns_as_string(notebook))
            
            
            if self.config_manager.is_rule_enabled("code_quality_validation", "column_nullability"):
                results.extend(self._validate_columns_not_null(notebook))
            
            # Code Quality Best Practices
            
            if self.config_manager.is_rule_enabled("code_quality_validation", "notebook_idempotency"):
                results.append(self._validate_notebook_idempotency(notebook))
            
            if self.config_manager.is_rule_enabled("code_quality_validation", "relative_notebook_references"):
                results.extend(self._validate_relative_notebook_references(notebook))
            
        
        return results
    
    # === LANGUAGE VALIDATION METHODS ===
    
    def _validate_valid_languages(self, notebook: NotebookMetadata) -> ValidationResult:
        """Validate that only approved languages are used."""
        if notebook.language not in self.valid_languages:
            return self.create_failed_result(
                "Valid Languages",
                f"Notebook language '{notebook.language}' is not in valid languages: {self.valid_languages}",
                notebook.path
            )
        
        return self.create_passed_result(
            "Valid Languages",
            f"Notebook uses valid language: {notebook.language}",
            notebook.path
        )
    
    # === STRUCTURE VALIDATION METHODS ===
    
    def _validate_header_section(self, notebook: NotebookMetadata) -> ValidationResult:
        """Validate that a header section exists."""
        first_cells = notebook.cells[:2]
        
        has_header = False
        for cell in first_cells:
            cell_content = "\n".join(cell.get("source", []))
            if "header" in cell_content.lower():
                has_header = True
                break
        
        if has_header:
            return self.create_passed_result(
                "Header Section",
                "Header section found in notebook",
                notebook.path
            )
        else:
            return self.create_failed_result(
                "Header Section",
                "No header section detected in first 2 cells",
                notebook.path
            )
    
    
    def _validate_command_cell_titles(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate that command cells have descriptive titles."""
        results = []
        
        titles = []
        
        for cell in notebook.cells:
            if cell.get("cell_type") == "code":
                title = cell.get("title", "").strip()
                if title:
                    titles.append(title)
                else:
                    results.append(self.create_failed_result(
                        "Command Cell Title",
                        "Command cell missing title",
                        notebook.path
                    ))
        
        if not titles:
            return results
        
        try:
            title_checks = self.llm_service.batch_is_descriptive_names(titles, batch_size=5)
            
            for i, (is_descriptive, explanation) in enumerate(title_checks):
                if i < len(titles):
                    title = titles[i]
                    if is_descriptive:
                        results.append(self.create_passed_result(
                            "Command Cell Title Descriptive",
                            f"Title '{title}': {explanation}",
                            notebook.path
                        ))
                    else:
                        results.append(self.create_failed_result(
                            "Command Cell Title Descriptive",
                            f"Title '{title}': {explanation}",
                            notebook.path
                        ))
                        
        except Exception as e:
            logger.error(f"Error validating titles: {e}")
            results.append(self.create_failed_result(
                "Command Cell Title Descriptive",
                f"Error checking title descriptiveness: {str(e)}",
                notebook.path
            ))
        
        return results
    
    
    
    
    
    
    # === DATABASE VALIDATION METHODS ===
    
    def _validate_character_columns_as_string(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate that character columns use STRING type."""
        results = []
        all_code = notebook.get_all_code()
        
        char_cols = re.findall(
            r'(varchar|char)\s*\(\d+\)', 
            all_code, 
            re.IGNORECASE
        )
        
        if char_cols:
            results.append(self.create_failed_result(
                "Character Columns as STRING",
                f"Found {len(char_cols)} VARCHAR/CHAR columns; use STRING instead",
                notebook.path
            ))
        else:
            has_create_table = bool(re.search(
                r'CREATE TABLE', 
                all_code, 
                re.IGNORECASE
            ))
            
            if has_create_table:
                results.append(self.create_passed_result(
                    "Character Columns as STRING",
                    "No VARCHAR/CHAR columns found",
                    notebook.path
                ))
        
        return results
    
    
    def _validate_columns_not_null(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate that columns are NOT NULL unless optional."""
        results = []
        all_code = notebook.get_all_code()
        
        table_matches = re.finditer(
            r'CREATE TABLE.*?\((.*?)\)', 
            all_code, 
            re.IGNORECASE | re.DOTALL
        )
        
        for match in table_matches:
            table_def = match.group(1)
            
            col_matches = re.finditer(
                r'(\w+)\s+(STRING|INT|TIMESTAMP|DATE|BIGINT|DECIMAL|BOOLEAN)\s*(NOT NULL)?', 
                table_def, 
                re.IGNORECASE
            )
            
            for col_match in col_matches:
                col_name = col_match.group(1)
                col_type = col_match.group(2)
                not_null = col_match.group(3)
                
                if not not_null and col_type.upper() != "STRING":
                    results.append(self.create_failed_result(
                        "Columns NOT NULL",
                        f"Column '{col_name}' is nullable but not confirmed optional",
                        notebook.path
                    ))
                else:
                    results.append(self.create_passed_result(
                        "Columns NOT NULL",
                        f"Column '{col_name}' has appropriate nullability",
                        notebook.path
                    ))
        
        return results
    
    
    # === CODE QUALITY BEST PRACTICES ===
    
    
    def _validate_notebook_idempotency(self, notebook: NotebookMetadata) -> ValidationResult:
        """Validate that notebook is idempotent and restartable."""
        all_code = notebook.get_all_code()
        
        try:
            is_restartable, details = self.llm_service.is_restartable(all_code)
            
            if is_restartable:
                return self.create_passed_result(
                    "Notebook Idempotency",
                    details,
                    notebook.path
                )
            else:
                return self.create_failed_result(
                    "Notebook Idempotency",
                    details,
                    notebook.path
                )
        except Exception as e:
            logger.error(f"Error validating idempotency: {e}")
            return self.create_failed_result(
                "Notebook Idempotency",
                f"Error checking idempotency: {str(e)}",
                notebook.path
            )
    
    def _validate_relative_notebook_references(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate that notebook calls use relative paths."""
        results = []
        all_code = notebook.get_all_code()
        
        notebook_calls = re.finditer(
            r'dbutils\.notebook\.run\s*\(\s*[\'"]([^\'"]+)[\'"]', 
            all_code, 
            re.IGNORECASE
        )
        
        for match in notebook_calls:
            path = match.group(1)
            
            if path.startswith("/"):
                results.append(self.create_failed_result(
                    "Relative Notebook References",
                    f"Absolute path '{path}' used; use relative references",
                    notebook.path
                ))
            else:
                results.append(self.create_passed_result(
                    "Relative Notebook References",
                    f"Relative path '{path}' used correctly",
                    notebook.path
                ))
        
        return results