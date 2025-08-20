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
            
            # Language Standards (2 rules)
            if self.config_manager.is_rule_enabled("code_quality_validation", "default_language_match"):
                results.append(self._validate_default_language_match(notebook))
            
            if self.config_manager.is_rule_enabled("code_quality_validation", "valid_languages_only"):
                results.append(self._validate_valid_languages(notebook))
            
            # Structure & Formatting (3 rules)
            if self.config_manager.is_rule_enabled("code_quality_validation", "header_section_exists"):
                results.append(self._validate_header_section(notebook))
            
            if self.config_manager.is_rule_enabled("code_quality_validation", "no_unnecessary_blank_space"):
                results.append(self._validate_no_unnecessary_blank_space(notebook))
            
            if self.config_manager.is_rule_enabled("code_quality_validation", "descriptive_cell_titles"):
                results.extend(self._validate_command_cell_titles(notebook))
            
            # Naming Conventions (2 rules)
            if self.config_manager.is_rule_enabled("code_quality_validation", "pascal_case_columns"):
                results.extend(self._validate_pascal_case_columns(notebook))
            
            if self.config_manager.is_rule_enabled("code_quality_validation", "table_name_convention"):
                results.extend(self._validate_table_name_convention(notebook))
            
            # Database Standards (6 rules)
            if self.config_manager.is_rule_enabled("code_quality_validation", "string_data_types"):
                results.extend(self._validate_character_columns_as_string(notebook))
            
            if self.config_manager.is_rule_enabled("code_quality_validation", "temporary_view_schema"):
                results.extend(self._validate_temporary_view_schema(notebook))
            
            if self.config_manager.is_rule_enabled("code_quality_validation", "fast_ingest_approval"):
                results.extend(self._validate_fast_ingest_usage(notebook))
            
            if self.config_manager.is_rule_enabled("code_quality_validation", "audit_columns"):
                results.extend(self._validate_audit_columns(notebook))
            
            if self.config_manager.is_rule_enabled("code_quality_validation", "viewer_ad_group_notification"):
                results.append(self._validate_viewer_ad_group_notification(notebook))
            
            if self.config_manager.is_rule_enabled("code_quality_validation", "column_nullability"):
                results.extend(self._validate_columns_not_null(notebook))
            
            # Code Quality Best Practices (5 rules)
            if self.config_manager.is_rule_enabled("code_quality_validation", "timezone_handling"):
                results.append(self._validate_time_zone_handling(notebook))
            
            if self.config_manager.is_rule_enabled("code_quality_validation", "notebook_restartability"):
                results.append(self._validate_notebook_restartability(notebook))
            
            if self.config_manager.is_rule_enabled("code_quality_validation", "initial_notebook_exists"):
                results.append(self._validate_initial_notebook_exists(notebook))
            
            if self.config_manager.is_rule_enabled("code_quality_validation", "relative_notebook_references"):
                results.extend(self._validate_relative_notebook_references(notebook))
            
            if self.config_manager.is_rule_enabled("code_quality_validation", "views_explicit_columns"):
                results.extend(self._validate_views_use_column_names(notebook))
        
        return results
    
    # === NAMING VALIDATION METHODS ===
    
    def _validate_pascal_case_columns(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate that column names follow PascalCase."""
        results = []
        all_code = notebook.get_all_code()
        
        create_table_matches = re.finditer(
            r'CREATE TABLE.*?\((.*?)\)', 
            all_code, 
            re.IGNORECASE | re.DOTALL
        )
        
        for match in create_table_matches:
            table_def = match.group(1)
            
            cols = re.findall(
                r'(\w+)\s+(STRING|INT|TIMESTAMP|DATE|BIGINT|DECIMAL|BOOLEAN)', 
                table_def, 
                re.IGNORECASE
            )
            
            for col_name, col_type in cols:
                if not self._is_pascal_case(col_name):
                    results.append(self.create_failed_result(
                        "PascalCase Column Names",
                        f"Column '{col_name}' does not follow PascalCase",
                        notebook.path
                    ))
                else:
                    results.append(self.create_passed_result(
                        "PascalCase Column Names",
                        f"Column '{col_name}' follows PascalCase",
                        notebook.path
                    ))
        
        return results
    
    def _validate_table_name_convention(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate that table names follow lowercase_with_underscores."""
        results = []
        all_code = notebook.get_all_code()
        
        table_matches = re.finditer(
            r'CREATE TABLE\s+(?:[a-z0-9_]+\.)?([a-z0-9_]+)', 
            all_code, 
            re.IGNORECASE
        )
        
        for match in table_matches:
            table_name = match.group(1)
            
            if not self._is_valid_table_name(table_name):
                results.append(self.create_failed_result(
                    "Table Name Convention",
                    f"Table '{table_name}' does not follow lowercase_with_underscores",
                    notebook.path
                ))
            else:
                results.append(self.create_passed_result(
                    "Table Name Convention",
                    f"Table '{table_name}' follows lowercase_with_underscores",
                    notebook.path
                ))
        
        return results
    
    def _is_pascal_case(self, name: str) -> bool:
        """Check if name follows PascalCase."""
        return bool(re.match(r'^[A-Z][a-zA-Z0-9]*$', name))
    
    def _is_valid_table_name(self, name: str) -> bool:
        """Check if table name is lowercase with underscores."""
        return bool(re.match(r'^[a-z0-9_]+$', name))
    
    def _validate_time_zone_handling(self, notebook: NotebookMetadata) -> ValidationResult:
        """Validate proper time zone handling."""
        all_code = notebook.get_all_code()
        
        has_time = bool(re.search(r'\b(time|timestamp|date)\b', all_code, re.IGNORECASE))
        
        if not has_time:
            return self.create_passed_result(
                "Time Zone Handling",
                "No time-related code detected",
                notebook.path
            )
        
        has_timezone = bool(re.search(
            r'(SET TIME ZONE|timezone|spark\.sql\.session\.timeZone)', 
            all_code, 
            re.IGNORECASE
        ))
        
        if has_timezone:
            return self.create_passed_result(
                "Time Zone Handling",
                "Time zone handling found in time-related code",
                notebook.path
            )
        else:
            return self.create_failed_result(
                "Time Zone Handling",
                "Time-related code detected but no time zone setting found",
                notebook.path
            )
    
    def _validate_notebook_restartability(self, notebook: NotebookMetadata) -> ValidationResult:
        """Validate that notebook is restartable."""
        all_code = notebook.get_all_code()
        
        try:
            is_restartable, details = self.llm_service.is_restartable(all_code)
            
            if is_restartable:
                return self.create_passed_result(
                    "Notebook Restartable",
                    details,
                    notebook.path
                )
            else:
                return self.create_failed_result(
                    "Notebook Restartable",
                    details,
                    notebook.path
                )
        except Exception as e:
            logger.error(f"Error validating restartability: {e}")
            return self.create_failed_result(
                "Notebook Restartable",
                f"Error checking restartability: {str(e)}",
                notebook.path
            )
    
    def _validate_initial_notebook_exists(self, notebook: NotebookMetadata) -> ValidationResult:
        """Validate that incremental notebooks have corresponding initial notebooks."""
        if "incremental" not in notebook.path.lower():
            return self.create_passed_result(
                "Initial Notebook Exists",
                "Not an incremental notebook",
                notebook.path
            )
        
        initial_path = notebook.path.replace("incremental", "initial")
        
        if self.notebook_service.notebook_exists(initial_path):
            return self.create_passed_result(
                "Initial Notebook Exists",
                f"Initial notebook {initial_path} exists",
                notebook.path
            )
        else:
            return self.create_failed_result(
                "Initial Notebook Exists",
                f"Initial notebook {initial_path} not found",
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
    
    # === NAMING VALIDATION METHODS ===
    
    def _validate_pascal_case_columns(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate that column names follow PascalCase."""
        results = []
        all_code = notebook.get_all_code()
        
        create_table_matches = re.finditer(
            r'CREATE TABLE.*?\((.*?)\)', 
            all_code, 
            re.IGNORECASE | re.DOTALL
        )
        
        for match in create_table_matches:
            table_def = match.group(1)
            
            cols = re.findall(
                r'(\w+)\s+(STRING|INT|TIMESTAMP|DATE|BIGINT|DECIMAL|BOOLEAN)', 
                table_def, 
                re.IGNORECASE
            )
            
            for col_name, col_type in cols:
                if not self._is_pascal_case(col_name):
                    results.append(self.create_failed_result(
                        "PascalCase Column Names",
                        f"Column '{col_name}' does not follow PascalCase",
                        notebook.path
                    ))
                else:
                    results.append(self.create_passed_result(
                        "PascalCase Column Names",
                        f"Column '{col_name}' follows PascalCase",
                        notebook.path
                    ))
        
        return results
    
    def _validate_table_name_convention(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate that table names follow lowercase_with_underscores."""
        results = []
        all_code = notebook.get_all_code()
        
        table_matches = re.finditer(
            r'CREATE TABLE\s+(?:[a-z0-9_]+\.)?([a-z0-9_]+)', 
            all_code, 
            re.IGNORECASE
        )
        
        for match in table_matches:
            table_name = match.group(1)
            
            if not self._is_valid_table_name(table_name):
                results.append(self.create_failed_result(
                    "Table Name Convention",
                    f"Table '{table_name}' does not follow lowercase_with_underscores",
                    notebook.path
                ))
            else:
                results.append(self.create_passed_result(
                    "Table Name Convention",
                    f"Table '{table_name}' follows lowercase_with_underscores",
                    notebook.path
                ))
        
        return results
    
    def _is_pascal_case(self, name: str) -> bool:
        """Check if name follows PascalCase."""
        return bool(re.match(r'^[A-Z][a-zA-Z0-9]*$', name))
    
    def _is_valid_table_name(self, name: str) -> bool:
        """Check if table name is lowercase with underscores."""
        return bool(re.match(r'^[a-z0-9_]+$', name))
    
    def _validate_views_use_column_names(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate that views use column names explicitly."""
        results = []
        all_code = notebook.get_all_code()
        
        view_matches = re.finditer(
            r'CREATE\s+(?:TEMPORARY\s+)?VIEW\s+\w+.*?AS\s+(SELECT.*?)(?:;|\s*$)', 
            all_code, 
            re.IGNORECASE | re.DOTALL
        )
        
        for match in view_matches:
            select_statement = match.group(1)
            
            if "SELECT *" in select_statement.upper():
                results.append(self.create_failed_result(
                    "Views Use Column Names",
                    "SELECT * used in view definition; specify column names",
                    notebook.path
                ))
            else:
                results.append(self.create_passed_result(
                    "Views Use Column Names",
                    "View uses explicit column names",
                    notebook.path
                ))
        
        return results
    
    # === NAMING VALIDATION METHODS ===
    
    def _validate_pascal_case_columns(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate that column names follow PascalCase."""
        results = []
        all_code = notebook.get_all_code()
        
        create_table_matches = re.finditer(
            r'CREATE TABLE.*?\((.*?)\)', 
            all_code, 
            re.IGNORECASE | re.DOTALL
        )
        
        for match in create_table_matches:
            table_def = match.group(1)
            
            cols = re.findall(
                r'(\w+)\s+(STRING|INT|TIMESTAMP|DATE|BIGINT|DECIMAL|BOOLEAN)', 
                table_def, 
                re.IGNORECASE
            )
            
            for col_name, col_type in cols:
                if not self._is_pascal_case(col_name):
                    results.append(self.create_failed_result(
                        "PascalCase Column Names",
                        f"Column '{col_name}' does not follow PascalCase",
                        notebook.path
                    ))
                else:
                    results.append(self.create_passed_result(
                        "PascalCase Column Names",
                        f"Column '{col_name}' follows PascalCase",
                        notebook.path
                    ))
        
        return results
    
    def _validate_table_name_convention(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate that table names follow lowercase_with_underscores."""
        results = []
        all_code = notebook.get_all_code()
        
        table_matches = re.finditer(
            r'CREATE TABLE\s+(?:[a-z0-9_]+\.)?([a-z0-9_]+)', 
            all_code, 
            re.IGNORECASE
        )
        
        for match in table_matches:
            table_name = match.group(1)
            
            if not self._is_valid_table_name(table_name):
                results.append(self.create_failed_result(
                    "Table Name Convention",
                    f"Table '{table_name}' does not follow lowercase_with_underscores",
                    notebook.path
                ))
            else:
                results.append(self.create_passed_result(
                    "Table Name Convention",
                    f"Table '{table_name}' follows lowercase_with_underscores",
                    notebook.path
                ))
        
        return results
    
    def _is_pascal_case(self, name: str) -> bool:
        """Check if name follows PascalCase."""
        return bool(re.match(r'^[A-Z][a-zA-Z0-9]*$', name))
    
    def _is_valid_table_name(self, name: str) -> bool:
        """Check if table name is lowercase with underscores."""
        return bool(re.match(r'^[a-z0-9_]+$', name))
    
    # === LANGUAGE VALIDATION METHODS ===
    
    def _validate_default_language_match(self, notebook: NotebookMetadata) -> ValidationResult:
        """Validate that notebook default language matches majority of code cells."""
        code_cells = notebook.get_code_cells()
        
        if not code_cells:
            return self.create_failed_result(
                "Default Language Match",
                "No code cells found in notebook",
                notebook.path
            )
        
        cell_languages = [notebook.language] * len(code_cells)  # Placeholder; enhance for mixed languages
        
        language_counts = Counter(cell_languages)
        majority_language = language_counts.most_common(1)[0][0]
        
        if notebook.language == majority_language:
            return self.create_passed_result(
                "Default Language Match",
                f"Notebook language: {notebook.language}, Majority cell language: {majority_language}",
                notebook.path
            )
        else:
            return self.create_failed_result(
                "Default Language Match",
                f"Notebook language: {notebook.language}, Majority cell language: {majority_language}",
                notebook.path
            )
    
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
    
    def _validate_no_unnecessary_blank_space(self, notebook: NotebookMetadata) -> ValidationResult:
        """Validate that there's no unnecessary blank space."""
        all_code = notebook.get_all_code()
        
        if not all_code.strip():
            return self.create_failed_result(
                "No Unnecessary Blank Space",
                "No code content found to analyze",
                notebook.path
            )
        
        try:
            has_blank_space, details = self.llm_service.has_unnecessary_blank_space(all_code)
            
            if has_blank_space:
                return self.create_failed_result(
                    "No Unnecessary Blank Space",
                    details,
                    notebook.path
                )
            else:
                return self.create_passed_result(
                    "No Unnecessary Blank Space",
                    details,
                    notebook.path
                )
        except Exception as e:
            logger.error(f"Error validating blank space: {e}")
            return self.create_failed_result(
                "No Unnecessary Blank Space",
                f"Error checking blank space: {str(e)}",
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
    
    # === NAMING VALIDATION METHODS ===
    
    def _validate_pascal_case_columns(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate that column names follow PascalCase."""
        results = []
        all_code = notebook.get_all_code()
        
        create_table_matches = re.finditer(
            r'CREATE TABLE.*?\((.*?)\)', 
            all_code, 
            re.IGNORECASE | re.DOTALL
        )
        
        for match in create_table_matches:
            table_def = match.group(1)
            
            cols = re.findall(
                r'(\w+)\s+(STRING|INT|TIMESTAMP|DATE|BIGINT|DECIMAL|BOOLEAN)', 
                table_def, 
                re.IGNORECASE
            )
            
            for col_name, col_type in cols:
                if not self._is_pascal_case(col_name):
                    results.append(self.create_failed_result(
                        "PascalCase Column Names",
                        f"Column '{col_name}' does not follow PascalCase",
                        notebook.path
                    ))
                else:
                    results.append(self.create_passed_result(
                        "PascalCase Column Names",
                        f"Column '{col_name}' follows PascalCase",
                        notebook.path
                    ))
        
        return results
    
    def _validate_table_name_convention(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate that table names follow lowercase_with_underscores."""
        results = []
        all_code = notebook.get_all_code()
        
        table_matches = re.finditer(
            r'CREATE TABLE\s+(?:[a-z0-9_]+\.)?([a-z0-9_]+)', 
            all_code, 
            re.IGNORECASE
        )
        
        for match in table_matches:
            table_name = match.group(1)
            
            if not self._is_valid_table_name(table_name):
                results.append(self.create_failed_result(
                    "Table Name Convention",
                    f"Table '{table_name}' does not follow lowercase_with_underscores",
                    notebook.path
                ))
            else:
                results.append(self.create_passed_result(
                    "Table Name Convention",
                    f"Table '{table_name}' follows lowercase_with_underscores",
                    notebook.path
                ))
        
        return results
    
    def _is_pascal_case(self, name: str) -> bool:
        """Check if name follows PascalCase."""
        return bool(re.match(r'^[A-Z][a-zA-Z0-9]*$', name))
    
    def _is_valid_table_name(self, name: str) -> bool:
        """Check if table name is lowercase with underscores."""
        return bool(re.match(r'^[a-z0-9_]+$', name))
        
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
    
    # === NAMING VALIDATION METHODS ===
    
    def _validate_pascal_case_columns(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate that column names follow PascalCase."""
        results = []
        all_code = notebook.get_all_code()
        
        create_table_matches = re.finditer(
            r'CREATE TABLE.*?\((.*?)\)', 
            all_code, 
            re.IGNORECASE | re.DOTALL
        )
        
        for match in create_table_matches:
            table_def = match.group(1)
            
            cols = re.findall(
                r'(\w+)\s+(STRING|INT|TIMESTAMP|DATE|BIGINT|DECIMAL|BOOLEAN)', 
                table_def, 
                re.IGNORECASE
            )
            
            for col_name, col_type in cols:
                if not self._is_pascal_case(col_name):
                    results.append(self.create_failed_result(
                        "PascalCase Column Names",
                        f"Column '{col_name}' does not follow PascalCase",
                        notebook.path
                    ))
                else:
                    results.append(self.create_passed_result(
                        "PascalCase Column Names",
                        f"Column '{col_name}' follows PascalCase",
                        notebook.path
                    ))
        
        return results
    
    def _validate_table_name_convention(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate that table names follow lowercase_with_underscores."""
        results = []
        all_code = notebook.get_all_code()
        
        table_matches = re.finditer(
            r'CREATE TABLE\s+(?:[a-z0-9_]+\.)?([a-z0-9_]+)', 
            all_code, 
            re.IGNORECASE
        )
        
        for match in table_matches:
            table_name = match.group(1)
            
            if not self._is_valid_table_name(table_name):
                results.append(self.create_failed_result(
                    "Table Name Convention",
                    f"Table '{table_name}' does not follow lowercase_with_underscores",
                    notebook.path
                ))
            else:
                results.append(self.create_passed_result(
                    "Table Name Convention",
                    f"Table '{table_name}' follows lowercase_with_underscores",
                    notebook.path
                ))
        
        return results
    
    def _is_pascal_case(self, name: str) -> bool:
        """Check if name follows PascalCase."""
        return bool(re.match(r'^[A-Z][a-zA-Z0-9]*$', name))
    
    def _is_valid_table_name(self, name: str) -> bool:
        """Check if table name is lowercase with underscores."""
        return bool(re.match(r'^[a-z0-9_]+$', name))
    
    # === DATABASE VALIDATION METHODS ===
    
    def _validate_character_columns_as_string(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate that character columns use STRING type."""
        results = []
        all_code = notebook.get_all_code()
        
        char_cols = re.findall(
            r'(varchar|char)\s*\(\d+\)', 
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
    
    def _validate_temporary_view_schema(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate that temporary views have proper schema definition."""
        results = []
        all_code = notebook.get_all_code()
        
        temp_view_matches = re.finditer(
            r'CREATE\s+TEMPORARY\s+VIEW\s+(\w+)\s*\(([^)]*)\)', 
            all_code, 
            re.IGNORECASE
        )
        
        for match in temp_view_matches:
            view_name = match.group(1)
            schema_def = match.group(2).strip()
            
            if not schema_def:
                results.append(self.create_failed_result(
                    "Temporary View Schema",
                    f"Temporary view '{view_name}' missing schema definition",
                    notebook.path
                ))
            else:
                results.append(self.create_passed_result(
                    "Temporary View Schema",
                    f"Temporary view '{view_name}' has schema definition",
                    notebook.path
                ))
        
        return results
    
    def _validate_fast_ingest_usage(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate Fast Ingest usage."""
        results = []
        all_code = notebook.get_all_code()
        
        has_copy_into = bool(re.search(r'COPY INTO', all_code, re.IGNORECASE))
        has_fast_ingest = bool(re.search(r'fast ingest', all_code, re.IGNORECASE))
        
        if has_copy_into or has_fast_ingest:
            results.append(self.create_failed_result(
                "Fast Ingest Usage",
                "Fast Ingest detected; requires PGM sign-off for non-BIS databases",
                notebook.path
            ))
        else:
            results.append(self.create_passed_result(
                "Fast Ingest Usage",
                "No Fast Ingest usage detected",
                notebook.path
            ))
        
        return results
    
    def _validate_audit_columns(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate that tables have required audit columns."""
        results = []
        all_code = notebook.get_all_code()
        
        table_matches = re.finditer(
            r'CREATE TABLE\s+(?:[a-z0-9_]+\.)?([a-z0-9_]+)\s*\((.*?)\)', 
            all_code, 
            re.IGNORECASE | re.DOTALL
        )
        
        for match in table_matches:
            table_name = match.group(1)
            table_def = match.group(2)
            
            required_cols = [f"{table_name.capitalize()}ID", "BISInsertedDateTime"]
            
            if re.search(r'UPDATE', all_code, re.IGNORECASE):
                required_cols.append("BISUpdatedDateTime")
            
            missing_cols = [col for col in required_cols if col not in table_def]
            
            if missing_cols:
                results.append(self.create_failed_result(
                    "Audit Columns",
                    f"Table '{table_name}' missing audit columns: {', '.join(missing_cols)}",
                    notebook.path
                ))
            else:
                results.append(self.create_passed_result(
                    "Audit Columns",
                    f"Table '{table_name}' has all required audit columns",
                    notebook.path
                ))
        
        return results
    
    def _validate_viewer_ad_group_notification(self, notebook: NotebookMetadata) -> ValidationResult:
        """Validate Viewer AD Group and BIS-AI-TEAM notification."""
        return self.create_pending_result(
            "Viewer AD Group & BIS-AI-TEAM Notification",
            "Manual verification required for AD group and notification",
            notebook.path
        )
    
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
