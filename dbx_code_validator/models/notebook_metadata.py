"""Data model for notebook metadata."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class NotebookMetadata:
    """
    Represents metadata about a Databricks notebook.
    
    Attributes:
        path: Full path to the notebook
        language: Primary language of the notebook
        format: Export format (e.g., JUPYTER, DBC)
        cells: List of notebook cells
        status: Status information from Databricks API
    """
    path: str
    language: str
    format: str
    cells: List[Dict[str, Any]]
    status: Optional[Dict[str, Any]] = None
    
    def get_code_cells(self) -> List[Dict[str, Any]]:
        """Get only the code cells from the notebook."""
        return [cell for cell in self.cells if cell.get("cell_type") == "code"]
    
    def get_markdown_cells(self) -> List[Dict[str, Any]]:
        """Get only the markdown cells from the notebook."""
        return [cell for cell in self.cells if cell.get("cell_type") == "markdown"]
    
    def get_all_code(self) -> str:
        """Get all code from the notebook as a single string."""
        code_cells = self.get_code_cells()
        return "\n".join(
            "\n".join(cell.get("source", [])) 
            for cell in code_cells
        )
    
    def get_cell_titles(self) -> List[str]:
        """Get titles of all cells that have them."""
        titles = []
        for cell in self.cells:
            title = cell.get("title", "").strip()
            if title:
                titles.append(title)
        return titles 