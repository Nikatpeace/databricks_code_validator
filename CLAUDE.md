# Databricks Code Validator - Claude Code Context

## Project Overview
This is a Databricks Code Validator - an AI-powered validation tool for Databricks notebooks that enforces code standards and best practices. The solution accelerator provides comprehensive validation across code quality, performance, security, and governance domains.

## Project Structure
```
databricks_code_validator/
├── src/databricks_code_validator/     # Main package source code
│   ├── cli.py                        # Command-line interface
│   ├── config/                       # Configuration modules
│   │   ├── yaml_config.py           # YAML configuration handling
│   │   └── llm_providers.py         # LLM provider configurations
│   ├── models/                       # Data models
│   │   ├── validation_result.py     # Validation result structures
│   │   └── notebook_metadata.py     # Notebook metadata models
│   ├── validators/                   # Validation modules
│   │   ├── code_quality_validator.py
│   │   ├── performance_validator.py
│   │   └── governance_validator.py
│   └── utils/                        # Utility modules
│       ├── spark_utils.py           # Spark integration utilities
│       └── logging_utils.py         # Logging configuration
├── config/
│   └── validation_rules.yaml        # Default validation rules
├── notebooks/                        # Example notebooks
├── requirements.txt                  # Python dependencies
├── setup.py                         # Package setup configuration
└── README.md                        # Comprehensive documentation
```

## Key Commands

#Please don't include any references to claude code when you push changes to git. ALso, Claude code's name should not show up as contributors on git

### Installation & Setup

#### Local Development
```bash
# Install in development mode
pip install -e .

# Or install dependencies first, then package
pip install -r requirements.txt
pip install -e .

# Create default configuration
databricks-code-validator create-config --output validation_rules.yaml
```

#### Databricks Environment
```bash
# Install dependencies and package
%pip install -r /Workspace/Users/your.name@company.com/databricks_code_validator/requirements.txt
%pip install -e /Workspace/Users/your.name@company.com/databricks_code_validator/
dbutils.library.restartPython()

# Import and use
from dbx_code_validator.main import DatabricksCodeValidator
```

#### Alternative Databricks Installation (if pip install -e fails)
```python
# Add source directory to Python path
import sys
sys.path.insert(0, '/Workspace/Users/your.name@company.com/databricks_code_validator/src')

# Import and use
from dbx_code_validator.main import DatabricksCodeValidator
```

### Validation Commands
```bash
# Validate single notebook
databricks-code-validator validate --notebook "/path/to/notebook"

# Validate multiple notebooks
databricks-code-validator validate --notebooks "/path/1" "/path/2" --output results.json

# Batch validation from file
databricks-code-validator validate --notebooks-file notebooks.txt

# Generate HTML report
databricks-code-validator validate --notebook "/path/to/notebook" --output report.html --format html

# Save to Delta table
databricks-code-validator validate --notebook "/path/to/notebook" --save-to-table validation_results
```

### Development Commands
```bash
# Build wheel for distribution
python setup.py bdist_wheel

# Debug mode
databricks-code-validator validate --notebook "/path/to/notebook" --log-level DEBUG
```

## Configuration

### LLM Providers
The tool supports multiple LLM providers through environment variables:

- **OpenAI**: `LLM_PROVIDER_TYPE=openai`
- **Azure OpenAI**: `LLM_PROVIDER_TYPE=azure_openai`
- **Anthropic**: `LLM_PROVIDER_TYPE=anthropic`
- **Databricks**: `LLM_PROVIDER_TYPE=databricks`

Required environment variables:
- `LLM_API_KEY`: API key for the chosen provider
- `LLM_ENDPOINT_URL`: Endpoint URL (for some providers)
- `LLM_MODEL_NAME`: Model name to use

### Validation Rules
Configuration is managed through `validation_rules.yaml` with categories:
- **Language Validation**: Default language matching, approved languages
- **Structure Validation**: Header sections, organization
- **Code Quality**: Naming conventions, best practices
- **Performance**: Query optimization, resource management
- **Security**: Secret detection, access controls
- **Governance**: Unity Catalog compliance, documentation

## Key Features
- AI-powered validation using LLM models
- Configurable YAML-based rules
- Multi-LLM support (OpenAI, Azure OpenAI, Anthropic, Databricks, Hugging Face)
- Batch processing capabilities
- Multiple output formats (JSON, CSV, HTML)
- Spark integration for Delta table storage
- CI/CD pipeline integration support

## Entry Points
- Main CLI: `dbx_code_validator.cli:main`
- Console script: `databricks-code-validator`

## Development Notes
- Python 3.8+ required
- Uses setuptools for packaging
- MIT License
- Designed for Databricks enterprise customers
- Focuses on defensive security and code quality validation
- NEVER use DBFS in Databricks - this pattern is legacy and should not be used
