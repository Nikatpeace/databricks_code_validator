# Databricks Code Validator - Solution Accelerator

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

## 🚀 Deployment Options

This solution accelerator provides **three production-ready deployment options** to fit different use cases:

### 🎯 Choose Your Deployment Strategy

| Option | Use Case | Complexity | Best For |
|--------|----------|------------|----------|
| **[Option 1: Python API in Notebook](#option-1-python-api-in-notebook)** | Interactive validation, development, personal use | ⭐ Simple | Data scientists, individual developers |
| **[Option 2: Scheduled Wheel Job](#option-2-scheduled-wheel-job)** | Automated nightly validation, production monitoring | ⭐⭐ Medium | Production teams, scheduled validation |
| **[Option 3: GitHub Actions CI/CD](#option-3-github-actions-cicd)** | PR validation, automated quality gates | ⭐⭐⭐ Advanced | DevOps teams, enterprise CI/CD |

---

## Option 1: Python API in Notebook

**Perfect for:** Interactive validation, development, and personal use cases.

### Quick Setup
1. Clone this repository to Databricks Repos
2. Open the guided notebook: `notebooks/01_Python_API_Quick_Start.py`
3. Follow the step-by-step instructions

### Installation
```bash
# In a Databricks notebook
%pip install -r ../requirements.txt
%pip install -e ..
dbutils.library.restartPython()
```

### Example Usage
```python
from databricks_code_validator.main import DatabricksCodeValidator
from databricks_code_validator.config.yaml_config import YamlConfigManager

# Initialize validator
config = YamlConfigManager("config/validation_rules.yaml")
validator = DatabricksCodeValidator(
    llm_endpoint_url="your-llm-endpoint",
    llm_token="your-api-key",
    workspace_client=WorkspaceClient(),
    config_manager=config
)

# Validate notebooks
results = validator.validate_notebook("/Users/your.email/notebook")
summary = validator.get_validation_summary(results)
print(f"Passed: {summary['passed_results']}, Failed: {summary['failed_results']}")
```

📖 **[Complete Option 1 Tutorial →](notebooks/01_Python_API_Quick_Start.py)**

---

## Option 2: Scheduled Wheel Job

**Perfect for:** Production teams needing automated, scheduled notebook validation.

### Overview
- Build Python wheel package
- Upload to Databricks Libraries
- Create scheduled job using Databricks Jobs UI
- Automatic validation with Delta table results

### Quick Setup
```bash
# 1. Build the wheel
python setup.py bdist_wheel

# 2. Upload wheel to Databricks Libraries
# (Use Databricks UI: Libraries → Upload Python Wheel)

# 3. Create job in Databricks Jobs UI:
# - Type: Python wheel
# - Package: databricks_code_validator
# - Entry Point: cli:main
# - Parameters: validate --notebooks /path1 /path2 --save-to-table results
```

### Key Features
- 🕒 **Scheduled execution** (nightly, weekly, custom)
- 📊 **Delta table integration** for monitoring
- 🔄 **Built-in retry logic** and error handling
- 📧 **Email notifications** on success/failure
- ⚡ **Scalable cluster compute**

📖 **[Complete Option 2 Guide →](docs/OPTION_2_WHEEL_JOB_DEPLOYMENT.md)**

---

## Option 3: GitHub Actions CI/CD

**Perfect for:** Enterprise teams wanting automated PR validation and quality gates.

### Overview
- Validates notebooks on every pull request
- Integrates with Databricks via APIs (no DBFS usage)
- Blocks PRs with validation failures
- Saves results to Delta tables for monitoring

### Quick Setup
1. **Configure GitHub Secrets:**
   ```
   DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
   DATABRICKS_TOKEN=dapi-your-token
   LLM_PROVIDER_TYPE=databricks
   LLM_ENDPOINT_URL=your-llm-endpoint
   ```

2. **Enable the Workflow:**
   - The workflow file is already included: `.github/workflows/validate-notebooks.yml`
   - Automatically triggers on PR creation/updates

3. **Configure Branch Protection:**
   - Require "Validate Notebooks on PR" status check
   - Block merging if validation fails

### Key Features
- 🔍 **Automatic PR validation** - No manual intervention
- 🚫 **Quality gates** - Block PRs with violations
- 📊 **Delta table tracking** - Monitor validation trends
- ⚡ **Fast feedback** - Results in minutes
- 🔄 **Zero infrastructure** - Uses GitHub Actions + Databricks APIs

📖 **[Complete Option 3 Guide →](docs/OPTION_3_GITHUB_ACTIONS_CICD.md)**

---

## Local Development Setup

For development and testing:

```bash
# Clone the repository
git clone https://github.com/your-org/databricks_code_validator.git
cd databricks_code_validator

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .
```

## ⚡ Quick Start Guide

### 1. Choose Your Deployment Option

Select the deployment option that best fits your needs:

- **🔍 Just want to try it?** → [Option 1: Python API in Notebook](#option-1-python-api-in-notebook)
- **⏰ Need scheduled validation?** → [Option 2: Scheduled Wheel Job](#option-2-scheduled-wheel-job)
- **🚀 Want CI/CD integration?** → [Option 3: GitHub Actions CI/CD](#option-3-github-actions-cicd)

### 2. Configure LLM Provider

Choose and configure your LLM provider:

```bash
# Option A: Databricks Foundation Models (Recommended)
export LLM_PROVIDER_TYPE=databricks
export LLM_API_KEY=dapi-your-databricks-token
export LLM_ENDPOINT_URL=https://your-workspace.../serving-endpoints/your-endpoint/invocations
export LLM_MODEL_NAME=databricks-dbrx-instruct

# Option B: OpenAI
export LLM_PROVIDER_TYPE=openai
export LLM_API_KEY=sk-your-openai-key
export LLM_MODEL_NAME=gpt-3.5-turbo

# Option C: Anthropic
export LLM_PROVIDER_TYPE=anthropic
export LLM_API_KEY=your-anthropic-key
export LLM_MODEL_NAME=claude-3-sonnet-20240229
```

### 3. Create Configuration

```bash
# Generate default validation rules
databricks-code-validator create-config --output validation_rules.yaml

# Customize the rules to match your organization's standards
```

### 4. Start Validating

#### Single Notebook
```bash
databricks-code-validator validate --notebook "/path/to/notebook"
```

#### Multiple Notebooks
```bash
databricks-code-validator validate --notebooks "/path/1" "/path/2" --output results.json
```

#### Batch Validation
```bash
# Create a file with notebook paths
echo "/path/to/notebook1" > notebooks.txt
echo "/path/to/notebook2" >> notebooks.txt

databricks-code-validator validate --notebooks-file notebooks.txt
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
databricks-code-validator validate --notebook "/path/to/notebook" --output results.json --format json
```

### CSV Output
```bash
databricks-code-validator validate --notebook "/path/to/notebook" --output results.csv --format csv
```

### HTML Report
```bash
databricks-code-validator validate --notebook "/path/to/notebook" --output report.html --format html
```

### Spark/Delta Table
```bash
databricks-code-validator validate --notebook "/path/to/notebook" --save-to-table validation_results
```

## 🔌 Enterprise Integration

### Production Deployment Patterns

#### Multi-Environment Setup
```yaml
# Deploy across dev/staging/prod environments
Environments:
  dev:
    databricks_host: https://dev-workspace.cloud.databricks.com
    validation_table: dev.governance.validation_results
  staging:
    databricks_host: https://staging-workspace.cloud.databricks.com
    validation_table: staging.governance.validation_results
  prod:
    databricks_host: https://prod-workspace.cloud.databricks.com
    validation_table: prod.governance.validation_results
```

#### Monitoring Dashboard Integration
```sql
-- Example queries for validation monitoring
SELECT
    DATE(validation_timestamp) as date,
    COUNT(*) as total_validations,
    SUM(CASE WHEN status = 'passed' THEN 1 ELSE 0 END) as passed,
    ROUND(100.0 * SUM(CASE WHEN status = 'passed' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate
FROM governance.validation_results
WHERE validation_timestamp >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY DATE(validation_timestamp)
ORDER BY date DESC;
```

#### Automated Remediation
```python
# Example: Auto-create tickets for validation failures
def create_jira_tickets_for_failures(validation_results):
    failed_results = [r for r in validation_results if r.status == 'Failed']
    for result in failed_results:
        create_jira_ticket(
            title=f"Code Standard Violation: {result.rule}",
            notebook=result.notebook_path,
            details=result.details
        )
```

## 🎯 Advanced Usage

### Custom Validation Rules
You can create custom validation rules by extending the base validator:

```python
from databricks_code_validator.validators.base_validator import BaseValidator
from databricks_code_validator.models.validation_result import ValidationResult

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
from databricks_code_validator.config.llm_providers import LLMProviderConfig, LLMProviderType

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
databricks-code-validator validate \
  --notebooks-file notebooks.txt \
  --output results.json \
  --format json

# Generate HTML summary
databricks-code-validator validate \
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
   databricks-code-validator examples --type llm
   ```

2. **Configuration Errors**
   ```bash
   # Validate configuration
   databricks-code-validator validate --config validation_rules.yaml --notebook "/path/to/test"
   ```

3. **Memory Issues with Large Notebooks**
   ```yaml
   # Adjust batch size in config
   global:
     batch_size: 2  # Reduce for large notebooks
   ```

### Debug Mode
```bash
databricks-code-validator validate --notebook "/path/to/notebook" --log-level DEBUG
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
1. Create a new validator in `src/databricks_code_validator/validators/`
2. Extend `BaseValidator`
3. Add configuration options to `validation_rules.yaml`
4. Add tests
5. Update documentation

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## ⚡ Performance

- **Validation Speed**: ~30 seconds per notebook (varies by LLM provider)
- **Concurrent Processing**: Supports batch validation of multiple notebooks
- **Resource Usage**: Optimized for standard Databricks clusters
- **Scalability**: Tested with 100+ notebooks in production environments


## 📚 Documentation

### Deployment Guides
- 📓 **[Option 1: Python API in Notebook](notebooks/01_Python_API_Quick_Start.py)** - Interactive validation tutorial
- ⚙️ **[Option 2: Scheduled Wheel Job](docs/OPTION_2_WHEEL_JOB_DEPLOYMENT.md)** - Production job setup guide
- 🚀 **[Option 3: GitHub Actions CI/CD](docs/OPTION_3_GITHUB_ACTIONS_CICD.md)** - CI/CD integration guide

### Additional Resources
- 🔧 **Configuration Reference**: `config/validation_rules.yaml`
- 🐛 **Troubleshooting**: See individual deployment guides
- 🔍 **API Reference**: Explore `src/databricks_code_validator/`

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

### Development Workflow
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

## 📞 Support

For issues and questions:
- 🐛 **[GitHub Issues](https://github.com/your-org/databricks_code_validator/issues)** - Report bugs or request features
- 📖 **[Documentation](docs/)** - Comprehensive guides and references
- 💬 **[Discussions](https://github.com/your-org/databricks_code_validator/discussions)** - Community support and questions

---

## 🎯 Ready to Get Started?

**Choose your deployment option:**
- 🔍 **Try it now**: [Python API in Notebook →](notebooks/01_Python_API_Quick_Start.py)
- ⏰ **Production setup**: [Scheduled Wheel Job →](docs/OPTION_2_WHEEL_JOB_DEPLOYMENT.md)
- 🚀 **Enterprise CI/CD**: [GitHub Actions Integration →](docs/OPTION_3_GITHUB_ACTIONS_CICD.md)

**Questions?** Check out our [documentation](docs/) or [open an issue](https://github.com/your-org/databricks_code_validator/issues).