"""Service for fetching notebook content from Databricks and managing related operations."""

import base64
import json
import logging
from typing import Dict, Any, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat
from pyspark.sql import DataFrame

from ..models.notebook_metadata import NotebookMetadata
from ..utils.logging_utils import get_logger

logger = get_logger(__name__)

class NotebookService:
    """Service for interacting with Databricks notebooks."""
    
    def __init__(self, workspace_client: Optional[WorkspaceClient] = None):
        """
        Initialize the notebook service.
        
        Args:
            workspace_client: Databricks workspace client.
                             Must be provided for proper authentication.
        """
        if workspace_client is None:
            raise ValueError(
                "WorkspaceClient is required for NotebookService. "
                "Please provide a properly authenticated WorkspaceClient instance."
            )
        self.workspace_client = workspace_client
    
    def fetch_notebook_content(self, notebook_path: str) -> Dict[str, Any]:
        """
        Fetch notebook content in JUPYTER format.
        
        Args:
            notebook_path: Path to the notebook in Databricks workspace
            
        Returns:
            Dict containing status, metadata, and content
        """
        try:
            # Normalize path
            if notebook_path.startswith("/Workspace/"):
                notebook_path = notebook_path.replace("/Workspace/", "/", 1)

            # Get notebook status
            status = self.workspace_client.workspace.get_status(notebook_path)
            logger.info(f"Notebook status: {status}")

            # Export the notebook in JUPYTER format
            logger.info("Exporting notebook using SDK (JUPYTER format)...")
            export_response = self.workspace_client.workspace.export(
                path=notebook_path,
                format=ExportFormat.JUPYTER
            )

            # Access content
            encoded = export_response.content
            decoded_content = base64.b64decode(encoded).decode("utf-8")

            # Parse the JUPYTER JSON
            notebook_json = json.loads(decoded_content)

            return {
                "status": "success",
                "metadata": {
                    "path": notebook_path,
                    "language": str(status.language),
                    "format": "JUPYTER"
                },
                "content": notebook_json
            }

        except Exception as e:
            logger.error(f"Failed to fetch notebook {notebook_path}: {str(e)}")
            return {
                "status": "error", 
                "message": f"Failed to fetch notebook: {str(e)}"
            }
    
    def create_notebook_metadata(self, notebook_path: str) -> Optional[NotebookMetadata]:
        """
        Create a NotebookMetadata object from a notebook path.
        
        Args:
            notebook_path: Path to the notebook
            
        Returns:
            NotebookMetadata object or None if failed
        """
        result = self.fetch_notebook_content(notebook_path)
        
        if result["status"] != "success":
            logger.error(f"Failed to create metadata for {notebook_path}")
            return None
        
        metadata = result["metadata"]
        content = result["content"]
        
        return NotebookMetadata(
            path=notebook_path,
            language=metadata.get("language", "").lower(),
            format=metadata.get("format", "JUPYTER"),
            cells=content.get("cells", [])
        )

    def create_notebook_metadata_from_content(self, notebook_path: str, content: str) -> Optional[NotebookMetadata]:
        """
        Create notebook metadata from provided content string.
        
        Args:
            notebook_path: Path to the notebook (for reference)
            content: The raw content string of the notebook (JSON format)
        
        Returns:
            NotebookMetadata object or None if creation fails
        """
        try:
            # Parse the JSON content
            notebook_json = json.loads(content)
            
            # Extract language from kernelspec
            language = notebook_json.get('metadata', {}).get('kernelspec', {}).get('language', 'python').lower()
            
            return NotebookMetadata(
                path=notebook_path,
                language=language,
                format="JUPYTER",
                cells=notebook_json.get("cells", [])
            )
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse notebook content for {notebook_path}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error creating metadata from content for {notebook_path}: {str(e)}")
            return None
    
    def notebook_exists(self, notebook_path: str) -> bool:
        """
        Check if a notebook exists at the given path.
        
        Args:
            notebook_path: Path to check
            
        Returns:
            True if notebook exists, False otherwise
        """
        try:
            self.workspace_client.workspace.get_status(notebook_path)
            return True
        except Exception:
            return False
    
    def save_validation_results(self, df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
        """
        Save validation results to a Delta table.
        
        Args:
            df: Spark DataFrame with validation results
            table_name: Name of the Delta table
            mode: Write mode (overwrite, append, etc.)
        """
        if df is None:
            logger.error("DataFrame cannot be None")
            raise ValueError("DataFrame cannot be None")
        
        try:
            # Use Databricks' pre-existing spark session
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA")
            df.write.format("delta").mode(mode).saveAsTable(table_name)
            logger.info(f"Successfully saved validation results to {table_name}")
        except Exception as e:
            logger.error(f"Failed to save validation results to {table_name}: {str(e)}")
            raise
