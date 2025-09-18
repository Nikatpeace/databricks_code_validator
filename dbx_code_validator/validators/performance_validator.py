"""Performance validator for checking performance-related rules."""

import re
from typing import List

from .base_validator import BaseValidator
from ..models.validation_result import ValidationResult
from ..models.notebook_metadata import NotebookMetadata
from ..services.llm_service import LLMService
from ..config.yaml_config import YamlConfigManager
from ..utils.logging_utils import get_logger

logger = get_logger(__name__)

class PerformanceValidator(BaseValidator):
    """Validator for performance rules."""
    
    def __init__(self, llm_service: LLMService, config_manager: YamlConfigManager = None):
        """Initialize the performance validator."""
        super().__init__("Performance Validator")
        self.llm_service = llm_service
        self.config_manager = config_manager or YamlConfigManager()
    
    def validate(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """
        Validate performance-related rules.
        
        Args:
            notebook: The notebook metadata to validate, containing code cells and path
            
        Returns:
            List of validation results
        """
        results = []
        config = self.config_manager.get_config()
        
        if config.performance_validation.enabled:
            logger.debug(f"Validating performance rules for {notebook.path}")
            
            if self.config_manager.is_rule_enabled("performance_validation", "avoid_select_star"):
                results.extend(self._validate_avoid_select_star(notebook))
            
            if self.config_manager.is_rule_enabled("performance_validation", "partitioning_and_clustering"):
                results.extend(self._validate_partitioning_and_clustering(notebook))
            
            if self.config_manager.is_rule_enabled("performance_validation", "efficient_joins"):
                results.append(self._validate_efficient_joins(notebook))
            
            if self.config_manager.is_rule_enabled("performance_validation", "python_vectorization"):
                results.extend(self._validate_python_vectorization(notebook))
            
            if self.config_manager.is_rule_enabled("performance_validation", "cache_usage"):
                results.append(self._validate_cache_usage(notebook))
            
            if self.config_manager.is_rule_enabled("performance_validation", "views_explicit_columns"):
                results.extend(self._validate_views_explicit_columns(notebook))
        
        return results
    
    def _validate_avoid_select_star(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate avoidance of SELECT * in queries."""
        results = []
        all_code = notebook.get_all_code()
        
        select_star_matches = re.finditer(r'SELECT\s+\*', all_code, re.IGNORECASE)
        
        for match in select_star_matches:
            results.append(self.create_failed_result(
                "Avoid SELECT *",
                "SELECT * detected; specify explicit columns to reduce data scanning",
                notebook.path
            ))
        
        if not results:
            results.append(self.create_passed_result(
                "Avoid SELECT *",
                "No SELECT * found in queries",
                notebook.path
            ))
        
        return results
    
    def _validate_partitioning_and_clustering(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate use of partitioning or clustering in tables."""
        results = []
        all_code = notebook.get_all_code()
        
        has_partition = bool(re.search(r'PARTITIONED BY', all_code, re.IGNORECASE))
        has_cluster = bool(re.search(r'CLUSTERED BY', all_code, re.IGNORECASE))
        
        if has_partition or has_cluster:
            results.append(self.create_passed_result(
                "Partitioning and Clustering",
                "Partitioning or clustering found in table definitions",
                notebook.path
            ))
        else:
            try:
                llm_prompt = self.config_manager.get_rule_parameters("performance_validation", "partitioning_and_clustering").get(
                    "llm_prompt", "Suggest optimal partitioning/clustering columns based on schema. Return 'True' if optimized, 'False' if not, with explanation."
                )
                llm_response = self.llm_service.call_llm(all_code, llm_prompt)
                details = llm_response["choices"][0]["message"]["content"]
                match = re.search(r'(True|False)\s*\((.*)\)', details.strip())
                if match and match.group(1).lower() == "true":
                    results.append(self.create_passed_result(
                        "Partitioning and Clustering",
                        match.group(2),
                        notebook.path
                    ))
                else:
                    results.append(self.create_failed_result(
                        "Partitioning and Clustering",
                        match.group(2) if match else "No partitioning or clustering found; use for performance on large tables",
                        notebook.path
                    ))
            except Exception as e:
                logger.error(f"Error validating partitioning: {e}")
                results.append(self.create_failed_result(
                    "Partitioning and Clustering",
                    "No partitioning or clustering found; use for performance on large tables",
                    notebook.path
                ))
        
        return results
    
    def _validate_efficient_joins(self, notebook: NotebookMetadata) -> ValidationResult:
        """Validate efficient joins."""
        all_code = notebook.get_all_code()
        
        try:
            is_efficient, details = self.llm_service.analyze_join_efficiency(all_code)
            if is_efficient:
                return self.create_passed_result(
                    "Efficient Joins",
                    details,
                    notebook.path
                )
            else:
                return self.create_failed_result(
                    "Efficient Joins",
                    details,
                    notebook.path
                )
        except Exception as e:
            logger.error(f"Error validating joins: {e}")
            return self.create_failed_result(
                "Efficient Joins",
                f"Error analyzing joins: {str(e)}",
                notebook.path
            )
    
    def _validate_python_vectorization(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate use of vectorized operations in Python instead of loops."""
        results = []
        all_code = notebook.get_all_code()
        
        loop_matches = re.finditer(r'for.*iterrows\(\)', all_code, re.IGNORECASE)
        
        for match in loop_matches:
            results.append(self.create_failed_result(
                "Python Vectorization",
                "Loop with iterrows() detected; use vectorized operations or Spark APIs",
                notebook.path
            ))
        
        if not results:
            results.append(self.create_passed_result(
                "Python Vectorization",
                "No loops with iterrows() found; vectorized operations likely used",
                notebook.path
            ))
        
        return results
    
    def _validate_cache_usage(self, notebook: NotebookMetadata) -> ValidationResult:
        """Validate use of caching for frequently reused DataFrames."""
        all_code = notebook.get_all_code()
        
        has_cache = bool(re.search(r'\.cache\(\)|CACHE TABLE', all_code, re.IGNORECASE))
        
        if has_cache:
            return self.create_passed_result(
                "Cache Usage",
                "Caching found for DataFrames or tables",
                notebook.path
            )
        else:
            try:
                llm_prompt = self.config_manager.get_rule_parameters("performance_validation", "cache_usage").get(
                    "llm_prompt", "Check if this code could benefit from caching. Return 'True' if caching is appropriate, 'False' if not needed, with explanation."
                )
                llm_response = self.llm_service.call_llm(all_code, llm_prompt)
                details = llm_response["choices"][0]["message"]["content"]
                match = re.search(r'(True|False)\s*\((.*)\)', details.strip())
                if match and match.group(1).lower() == "true":
                    return self.create_passed_result(
                        "Cache Usage",
                        match.group(2),
                        notebook.path
                    )
                else:
                    return self.create_failed_result(
                        "Cache Usage",
                        match.group(2) if match else "No caching detected; consider for reused DataFrames",
                        notebook.path
                    )
            except Exception as e:
                logger.error(f"Error validating cache usage: {e}")
                return self.create_failed_result(
                    "Cache Usage",
                    "No caching detected; consider for reused DataFrames",
                    notebook.path
                )
    
    def _validate_views_explicit_columns(self, notebook: NotebookMetadata) -> List[ValidationResult]:
        """Validate that views specify column names explicitly."""
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
                    "Views Explicit Columns",
                    "SELECT * used in view definition; specify column names",
                    notebook.path
                ))
            else:
                results.append(self.create_passed_result(
                    "Views Explicit Columns",
                    "View uses explicit column names",
                    notebook.path
                ))
        
        return results