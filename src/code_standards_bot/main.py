"""Main CodeStandardsBot class."""

import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import uuid

from .models.validation_result import ValidationResult, ValidationStatus
from .models.notebook_metadata import NotebookMetadata
from .services.notebook_service import NotebookService
from .services.llm_service import LLMService
from .validators.code_quality_validator import CodeQualityValidator
from .validators.performance_validator import PerformanceValidator
from .validators.security_validator import SecurityValidator
from .validators.governance_validator import GovernanceValidator

logger = logging.getLogger(__name__)


class CodeStandardsBot:
    """Main class for validating Databricks notebooks against code standards."""
    
    def __init__(
        self,
        llm_endpoint_url: str,
        llm_token: str,
        workspace_client=None,
        config_manager=None
    ):
        """
        Initialize the CodeStandardsBot.
        
        Args:
            llm_endpoint_url: URL of the LLM endpoint
            llm_token: Authentication token for LLM
            workspace_client: Optional Databricks workspace client
            config_manager: Optional configuration manager for validation rules
        """
        self.notebook_service = NotebookService(workspace_client)
        self.llm_service = LLMService(llm_endpoint_url, llm_token)
        self.config_manager = config_manager
        
        # Initialize validators - clean 4-domain architecture
        self.validators = [
            # Code Quality Domain (18 rules: language, structure, naming, database, quality)
            CodeQualityValidator(self.llm_service, self.notebook_service, config_manager),
            
            # Performance Domain (6 rules)
            PerformanceValidator(self.llm_service, config_manager),
            
            # Security Domain (5 rules)  
            SecurityValidator(self.llm_service, config_manager),
            
            # Governance Domain (5 rules)
            GovernanceValidator(self.llm_service, config_manager)
        ]
    
    def validate_notebook(self, notebook_path: str, content: Optional[str] = None) -> List[ValidationResult]:
        """
        Validate a single notebook against all rules.
        
        Args:
            notebook_path: Path to the notebook to validate
            content: Optional pre-fetched content of the notebook (as string)
            
        Returns:
            List of validation results
        """
        logger.info(f"Starting validation for notebook: {notebook_path}")
        
        try:
            if content is None:
                # Fetch content if not provided
                notebook_metadata = self.notebook_service.create_notebook_metadata(notebook_path)
                if not notebook_metadata:
                    raise ValueError(f"Failed to fetch notebook {notebook_path}")
            else:
                # Use provided content and create metadata (adjust based on your NotebookService implementation)
                notebook_metadata = self.notebook_service.create_notebook_metadata_from_content(notebook_path, content)
                if not notebook_metadata:
                    raise ValueError(f"Failed to create metadata from content for {notebook_path}")
                
            # Run all validators
            all_results = []
            
            for validator in self.validators:
                try:
                    logger.info(f"Running {validator.name} on {notebook_path}")
                    results = validator.validate(notebook_metadata)
                    all_results.extend(results)
                    logger.info(f"{validator.name} completed with {len(results)} results")
                except Exception as e:
                    logger.error(f"Error running {validator.name}: {str(e)}")
                    error_result = ValidationResult(
                        rule=f"{validator.name} Error",
                        status=ValidationStatus.FAILED,
                        details=f"Error running validator: {str(e)}",
                        notebook_path=notebook_path,
                        audit_time=datetime.utcnow(),
                        audit_id=str(uuid.uuid4())
                    )
                    all_results.append(error_result)
            
            logger.info(f"Validation completed for {notebook_path}: {len(all_results)} total results")
            return all_results
        
        except Exception as e:
            logger.error(f"Failed to validate notebook {notebook_path}: {str(e)}")
            error_result = ValidationResult(
                rule="Notebook Access",
                status=ValidationStatus.FAILED,
                details=f"Failed to process notebook {notebook_path}: {str(e)}",
                notebook_path=notebook_path,
                audit_time=datetime.utcnow(),
                audit_id=str(uuid.uuid4())
            )
            return [error_result]
    
    def validate_notebooks(self, notebook_paths: List[str]) -> List[ValidationResult]:
        """
        Validate multiple notebooks.
        
        Args:
            notebook_paths: List of notebook paths to validate
            
        Returns:
            List of validation results from all notebooks
        """
        all_results = []
        
        for notebook_path in notebook_paths:
            results = self.validate_notebook(notebook_path)
            all_results.extend(results)
        
        return all_results
    
    def get_validation_summary(self, results: List[ValidationResult]) -> Dict[str, Any]:
        """
        Get a summary of validation results.
        
        Args:
            results: List of validation results
            
        Returns:
            Dictionary with summary statistics
        """
        total_results = len(results)
        passed_results = sum(1 for r in results if r.status.value == "Passed")
        failed_results = sum(1 for r in results if r.status.value == "Failed")
        pending_results = sum(1 for r in results if r.status.value == "Pending")
        
        # Group by rule
        rule_summary = {}
        for result in results:
            rule = result.rule
            if rule not in rule_summary:
                rule_summary[rule] = {"passed": 0, "failed": 0, "pending": 0}
            
            status = result.status.value.lower()
            rule_summary[rule][status] += 1
        
        # Group by notebook
        notebook_summary = {}
        for result in results:
            notebook = result.notebook_path
            if notebook not in notebook_summary:
                notebook_summary[notebook] = {"passed": 0, "failed": 0, "pending": 0}
            
            status = result.status.value.lower()
            notebook_summary[notebook][status] += 1
        
        return {
            "total_results": total_results,
            "passed_results": passed_results,
            "failed_results": failed_results,
            "pending_results": pending_results,
            "pass_rate": passed_results / total_results if total_results > 0 else 0,
            "rule_summary": rule_summary,
            "notebook_summary": notebook_summary
        }
    
    def create_spark_dataframe(self, results: List[ValidationResult], spark_session):
        """
        Create a Spark DataFrame from validation results.
        
        Args:
            results: List of validation results
            spark_session: Spark session object
            
        Returns:
            Spark DataFrame with validation results
        """
        # Convert results to dictionaries
        result_dicts = [result.to_dict() for result in results]
        
        # Create DataFrame
        df = spark_session.createDataFrame(result_dicts)
        
        return df
    
    def run_validation(self, notebook_path: str, spark_session=None) -> Dict[str, Any]:
        """
        Run validation on a notebook and return results with summary.
        
        Args:
            notebook_path: Path to the notebook to validate
            spark_session: Optional Spark session for DataFrame creation
            
        Returns:
            Dictionary with results and summary
        """
        results = self.validate_notebook(notebook_path)
        summary = self.get_validation_summary(results)
        
        output = {
            "notebook_path": notebook_path,
            "results": results,
            "summary": summary
        }
        
        if spark_session:
            output["dataframe"] = self.create_spark_dataframe(results, spark_session)
        
        return output 