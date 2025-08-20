"""
Code Standards Bot - Databricks notebook validation tool.

This package provides tools to validate Databricks notebooks against
predefined code standards and best practices.
"""

__version__ = "1.0.0"
__author__ = "Nikhil Chandna"

from .main import CodeStandardsBot
from .models.validation_result import ValidationResult
from .models.notebook_metadata import NotebookMetadata

__all__ = ["CodeStandardsBot", "ValidationResult", "NotebookMetadata"] 