# Code Standards Bot - Solution Accelerator

An AI-powered validation tool for Databricks notebooks that enforces code standards and best practices. This solution accelerator provides a comprehensive, configurable system for validating notebooks against customizable rules.

## 🚀 Features

### Core Capabilities
- **AI-Powered Validation**: Uses LLM models to intelligently analyze code patterns
- **Configurable Rules**: YAML-based configuration for customizing validation rules
- **Multi-LLM Support**: Works with OpenAI, Azure OpenAI, Anthropic, Databricks, and Hugging Face
- **Batch Processing**: Validate multiple notebooks efficiently
- **Multiple Output Formats**: JSON, CSV, HTML reports
- **Spark Integration**: Save results to Delta tables

### Validation Domains
The solution accelerator provides comprehensive validation across four key domains:

#### 🔧 **Code Quality Validation** (10+ rules)
- **Language Standards**: Consistent language usage and approved language validation
- **Structure & Formatting**: Notebook organization, header sections, cell titles, and spacing
- **Naming Conventions**: PascalCase columns, table naming patterns, and consistency
- **Database Standards**: STRING data types, schema definitions, audit columns, and nullability
- **Best Practices**: Timezone handling, restartability, and relative path references

#### ⚡ **Performance Validation** (6+ rules)
- **Query Optimization**: Avoid SELECT *, efficient joins, and broadcast hints
- **Data Architecture**: Partitioning and clustering strategies for large tables  
- **Python Efficiency**: Vectorized operations instead of loops with iterrows()
- **Resource Management**: Caching for reused DataFrames and memory optimization
- **View Optimization**: Explicit column specifications in views

#### 🔒 **Security Validation** (5+ rules)
- **Secret Management**: Detection of hard-coded credentials and tokens
- **Access Controls**: Proper GRANT/REVOKE usage and Unity Catalog enforcement
- **SQL Security**: SQL injection prevention and parameterized queries
- **Cluster Isolation**: Single-user mode requirements for sensitive operations
- **Data Encryption**: Storage and transmission encryption validation

#### 📊 **Governance Validation** (5+ rules)
- **Unity Catalog**: Three-level namespace enforcement and metastore compliance
- **Documentation**: Code comments, data lineage, and maintainability standards
- **Version Control**: Git integration and collaborative development practices
- **Data Isolation**: Environment separation (dev/staging/prod) for data paths
- **Metadata Management**: Required tagging for ownership, classification, and purpose

## 🌟 Why Use This Accelerator?

Designed for Databricks enterprise customers to automate code validation before production deployment. It reduces the workload on senior developers by flagging issues early, allowing faster reviews or less manual oversight. Key benefits include:
- **Cybersecurity Enhancements**: Detect risks like hard-coded credentials or vulnerabilities via AI-powered rules.
- **Code Quality and Efficiency**: Enforce best practices for performance, structure, and naming.
- **Customization**: Bring your own YAML rules for organization-specific standards.
- **Integration**: Save results to Delta tables for BI dashboards and integrate with CI/CD pipelines via Databricks Jobs.

## 📦 Installation

For Databricks enterprise users, we recommend the following methods to deploy this solution accelerator.


### 1. Installation in Databricks (Recommended)
- **Clone the Repository**: Use Databricks Repos to clone `https://github.com/Nikatpeace/Code_standards_validation_bot`. Path: `/Workspace/Repos/your-username/Code_standards_validation_bot`.
- **Install in a Notebook (Quick Start)**:
  ```bash
  %pip install -r /Workspace/Repos/your-username/Code_standards_validation_bot/requirements.txt
  %pip install -e /Workspace/Repos/your-username/Code_standards_validation_bot
  dbutils.library.restartPython()
  ```
- **Build and Upload Wheel (For Cluster-Wide Use)**:
  - Locally: `python setup.py bdist_wheel`
  - Upload the `.whl` file from `dist/` to Databricks Libraries (Libraries > Install New > Upload Python Egg or PyPI) and install on clusters or jobs.


### 2. Local Development Setup (Optional for Testing)
```bash
# Clone the repository
git clone https://github.com/Nikatpeace/Code_standards_validation_bot.git
cd Code_standards_validation_bot

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .
```

## 🛠️ Quick Start


#### Manual Configuration
Create a `validation_rules.yaml` file:
```bash
code-standards-bot create-config --output validation_rules.yaml
```

Set environment variables:
```bash
export LLM_PROVIDER_TYPE=openai
export LLM_API_KEY=your_api_key
export LLM_ENDPOINT_URL=https://api.openai.com/v1/chat/completions
export LLM_MODEL_NAME=gpt-3.5-turbo
```

Run the Quick-Start Notebook
For a guided setup and validation, run the included notebook:

Path: /notebooks/RUNME.py.ipynb
This notebook installs dependencies, configures the bot, and validates a sample notebook, saving results to JSON and a Delta table.

### 2. Validate Notebooks

#### Single Notebook
```bash
code-standards-bot validate --notebook "/path/to/notebook"
```


#### Multiple Notebooks
```bash
code-standards-bot validate --notebooks "/path/1" "/path/2" --output results.json
```

#### Batch Validation from File
```bash
# Create a file with notebook paths
echo "/path/to/notebook1" > notebooks.txt
echo "/path/to/notebook2" >> notebooks.txt

code-standards-bot validate --notebooks-file notebooks.txt
```

## 🔧 Configuration

### LLM Provider Configuration

#### OpenAI
```bash
export LLM_PROVIDER_TYPE=openai
export LLM_API_KEY=sk-your-openai-key
export LLM_MODEL_NAME=gpt-3.5-turbo
```

#### Azure OpenAI
```bash
export LLM_PROVIDER_TYPE=azure_openai
export LLM_API_KEY=your-azure-key
export LLM_ENDPOINT_URL=https://your-resource.openai.azure.com/openai/deployments/your-deployment/chat/completions
```

#### Anthropic
```bash
export LLM_PROVIDER_TYPE=anthropic
export LLM_API_KEY=your-anthropic-key
export LLM_MODEL_NAME=claude-3-sonnet-20240229
```

#### Databricks
```bash
export LLM_PROVIDER_TYPE=databricks
export LLM_API_KEY=dapi-your-databricks-token
export LLM_ENDPOINT_URL=https://your-workspace.cloud.databricks.com/serving-endpoints/your-endpoint/invocations
```

### Validation Rules Configuration

The `validation_rules.yaml` file allows you to customize which rules to run:

```yaml
# Global Configuration
global:
  enabled: true
  batch_size: 5
  max_retries: 3
  timeout_seconds: 30

# Language Validation Rules
language_validation:
  enabled: true
  rules:
    default_language_match:
      enabled: true
      description: "Notebook default language should match majority of code cells"
      severity: "error"
      
    valid_languages_only:
      enabled: true
      description: "Only approved languages should be used"
      severity: "error"
      parameters:
        allowed_languages: ["sql", "python", "scala", "r"]
        strict_mode: false

# Structure Validation Rules
structure_validation:
  enabled: true
  rules:
    header_section_exists:
      enabled: true
      description: "Notebook should have a header section"
      severity: "warning"
      parameters:
        header_keywords: ["header", "title", "description", "purpose"]
        check_first_n_cells: 3

# Custom Rules
custom_validation:
  enabled: true
  rules:
    company_specific_naming:
      enabled: false
      description: "Company-specific naming conventions"
      severity: "warning"
      type: "regex"
      parameters:
        patterns:
          table_names: "^(prod|dev|test)_.*"
          column_names: "^[A-Z][a-zA-Z0-9]*$"
```

## 📊 Output Formats

### JSON Output
```bash
code-standards-bot validate --notebook "/path/to/notebook" --output results.json --format json
```

### CSV Output
```bash
code-standards-bot validate --notebook "/path/to/notebook" --output results.csv --format csv
```

### HTML Report
```bash
code-standards-bot validate --notebook "/path/to/notebook" --output report.html --format html
```

### Spark/Delta Table
```bash
code-standards-bot validate --notebook "/path/to/notebook" --save-to-table validation_results
```

## 🔌 Integration Examples

### CI/CD Pipeline Integration
```yaml
# .github/workflows/validate-notebooks.yml
name: Validate Notebooks
on: [push, pull_request]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
          
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install -e .
          
      - name: Run validation
        run: |
          code-standards-bot validate --notebooks-file notebooks.txt --fail-on-errors
        env:
          LLM_API_KEY: ${{ secrets.LLM_API_KEY }}
          LLM_PROVIDER_TYPE: openai
```

### Databricks Job Integration
```python
# Databricks notebook cell
%pip install code-standards-bot

# Configure and run validation
from code_standards_bot.main import CodeStandardsBot
from code_standards_bot.config.yaml_config import YamlConfigManager

config_manager = YamlConfigManager("/Workspace/Repos/your-username/Code_standards_validation_bot/validation_rules.yaml")
bot = CodeStandardsBot(
    llm_endpoint_url=dbutils.secrets.get("llm", "endpoint_url"),
    llm_token=dbutils.secrets.get("llm", "api_key"),
    config_manager=config_manager
)

results = bot.validate_notebook("/path/to/notebook")
df = bot.create_spark_dataframe(results, spark)
df.write.format("delta").mode("overwrite").saveAsTable("validation_results")
```

## 🎯 Advanced Usage

### Custom Validation Rules
You can create custom validation rules by extending the base validator:

```python
from code_standards_bot.validators.base_validator import BaseValidator
from code_standards_bot.models.validation_result import ValidationResult

class MyCustomValidator(BaseValidator):
    def __init__(self, config_manager=None):
        super().__init__("My Custom Validator")
        self.config_manager = config_manager
    
    def validate(self, notebook):
        # Your custom validation logic
        return [
            self.create_passed_result(
                "Custom Rule",
                "Custom validation passed",
                notebook.path
            )
        ]
```

### Extending LLM Providers
Add support for new LLM providers:

```python
from code_standards_bot.config.llm_providers import LLMProviderConfig, LLMProviderType

def create_my_provider_config(api_key: str, endpoint: str) -> LLMProviderConfig:
    return LLMProviderConfig(
        provider_type=LLMProviderType.CUSTOM,
        endpoint_url=endpoint,
        api_key=api_key,
        model_name="my-custom-model"
    )
```

## 📈 Monitoring and Reporting

### Generate Summary Reports
```bash
code-standards-bot validate \
  --notebooks-file notebooks.txt \
  --output results.json \
  --format json

# Generate HTML summary
code-standards-bot validate \
  --notebooks-file notebooks.txt \
  --output report.html \
  --format html
```

### Integration with Monitoring Systems
```python
# Send results to monitoring system
import requests
import json

def send_to_monitoring(results):
    summary = bot.get_validation_summary(results)
    
    # Send to your monitoring system
    requests.post("https://your-monitoring-system.com/api/metrics", 
                  json=summary)
```

## 🔍 Troubleshooting

### Common Issues

1. **LLM Connection Issues**
   ```bash
   # Test LLM connection
   code-standards-bot examples --type llm
   ```

2. **Configuration Errors**
   ```bash
   # Validate configuration
   code-standards-bot validate --config validation_rules.yaml --notebook "/path/to/test"
   ```

3. **Memory Issues with Large Notebooks**
   ```yaml
   # Adjust batch size in config
   global:
     batch_size: 2  # Reduce for large notebooks
   ```

### Debug Mode
```bash
code-standards-bot validate --notebook "/path/to/notebook" --log-level DEBUG
```

## 🤝 Contributing

### Development Setup
```bash
# Install development dependencies
pip install -r requirements.txt
pip install -e ".[dev]"

# Run tests
pytest tests/

# Format code
black src/

# Lint code
flake8 src/
```

### Adding New Validators
1. Create a new validator in `src/code_standards_bot/validators/`
2. Extend `BaseValidator`
3. Add configuration options to `validation_rules.yaml`
4. Add tests
5. Update documentation

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.


## 📞 Support

For issues and questions:
- GitHub Issues: [Report a bug or request a feature](https://github.com/Nikatpeace/Code_standards_validation_bot/issues)
- Documentation: [Wiki](https://github.com/Nikatpeace/Code_standards_validation_bot/wiki)
- Email: support@your-company.com

---

**Ready to enforce code standards at scale? Get started with the setup wizard:**

```bash
python scripts/setup_wizard.py
```