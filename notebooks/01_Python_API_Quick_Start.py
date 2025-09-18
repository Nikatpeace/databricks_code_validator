# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Code Validator - Python API Quick Start
# MAGIC
# MAGIC This notebook demonstrates **Option 1: Python API in Notebook** deployment.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC 1. Clone this repository to your Databricks workspace using Repos
# MAGIC 2. Configure your LLM provider credentials in Databricks Secrets
# MAGIC 3. Ensure you have appropriate cluster permissions
# MAGIC
# MAGIC ## What This Demo Shows
# MAGIC - ✅ Installation and setup
# MAGIC - ✅ Python API usage (recommended approach)
# MAGIC - ✅ Configuration management
# MAGIC - ✅ Single notebook validation
# MAGIC - ✅ Batch notebook validation
# MAGIC - ✅ Writing results to Delta tables
# MAGIC - ✅ Error handling and debugging

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Installation
# MAGIC
# MAGIC Install the validator from your cloned repository:

# COMMAND ----------

# Install dependencies and the validator
%pip install -r ../requirements.txt
%pip install -e ..
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Configure LLM Provider
# MAGIC
# MAGIC **Important**: Replace the placeholder values below with your actual configuration.
# MAGIC
# MAGIC ### Supported LLM Providers:
# MAGIC - `databricks` - Databricks Foundation Model APIs (Recommended)
# MAGIC - `openai` - OpenAI GPT models
# MAGIC - `azure_openai` - Azure OpenAI Service
# MAGIC - `anthropic` - Anthropic Claude models

# COMMAND ----------

import os
import time

# Configure your LLM provider (choose one option below)

# Option A: Databricks Foundation Models (Recommended)
os.environ['LLM_PROVIDER_TYPE'] = 'databricks'
os.environ['LLM_API_KEY'] = dbutils.secrets.get(scope="your_scope", key="databricks_pat_token")
os.environ['LLM_ENDPOINT_URL'] = 'https://your-workspace.cloud.databricks.com/serving-endpoints/your-endpoint/invocations'
os.environ['LLM_MODEL_NAME'] = 'databricks-dbrx-instruct'  # or 'databricks-llama-2-70b-chat'

# Option B: OpenAI (uncomment to use)
# os.environ['LLM_PROVIDER_TYPE'] = 'openai'
# os.environ['LLM_API_KEY'] = dbutils.secrets.get(scope="your_scope", key="openai_api_key")
# os.environ['LLM_MODEL_NAME'] = 'gpt-3.5-turbo'

# Option C: Anthropic (uncomment to use)
# os.environ['LLM_PROVIDER_TYPE'] = 'anthropic'
# os.environ['LLM_API_KEY'] = dbutils.secrets.get(scope="your_scope", key="anthropic_api_key")
# os.environ['LLM_MODEL_NAME'] = 'claude-3-sonnet-20240229'

print("✅ LLM provider configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Initialize Validator (Python API - Recommended)
# MAGIC
# MAGIC This approach gives you maximum flexibility and control:

# COMMAND ----------

# Import the validator and configuration classes
from databricks_code_validator.main import DatabricksCodeValidator
from databricks_code_validator.config.yaml_config import YamlConfigManager
from databricks_code_validator.config.llm_providers import get_llm_provider
from databricks.sdk import WorkspaceClient

# Load configuration
config_path = "/Workspace/Repos/your-username/databricks_code_validator/config/validation_rules.yaml"
config = YamlConfigManager(config_path)

# Get LLM configuration
try:
    llm_cfg = get_llm_provider()  # Reads from environment variables
    print(f"✅ Using LLM provider: {llm_cfg.provider_type}")
    print(f"✅ Model: {llm_cfg.model_name}")
except Exception as e:
    print(f"❌ LLM configuration error: {e}")
    print("Please check your environment variables and secrets configuration")
    raise

# Initialize Databricks workspace client
workspace_client = WorkspaceClient()

# Create validator instance
validator = DatabricksCodeValidator(
    llm_endpoint_url=llm_cfg.endpoint_url,
    llm_token=llm_cfg.api_key,
    workspace_client=workspace_client,
    config_manager=config
)

print("✅ Validator initialized successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Validate a Single Notebook

# COMMAND ----------

# Replace with the path to a notebook you want to validate
# Note: Use the workspace path without /Workspace prefix
notebook_path = "/Users/your.email@company.com/sample_notebook"

try:
    print(f"🔍 Validating notebook: {notebook_path}")

    # Run validation
    results = validator.validate_notebook(notebook_path)

    # Get summary
    summary = validator.get_validation_summary(results)

    print(f"\n📊 Validation Summary:")
    print(f"   ✅ Passed: {summary['passed_results']}")
    print(f"   ❌ Failed: {summary['failed_results']}")
    print(f"   ⏭️  Skipped: {summary.get('skipped_results', 0)}")

    # Display results as DataFrame for easy viewing
    df = validator.create_spark_dataframe(results, spark)
    display(df)

except Exception as e:
    print(f"❌ Validation error: {e}")
    print("\nTroubleshooting tips:")
    print("1. Verify the notebook path exists")
    print("2. Check your LLM provider credentials")
    print("3. Ensure the notebook is accessible")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Batch Validation (Multiple Notebooks)

# COMMAND ----------

# List of notebooks to validate (replace with your actual notebook paths)
notebooks_to_validate = [
    "/Users/your.email@company.com/notebook1",
    "/Users/your.email@company.com/notebook2",
    "/Users/your.email@company.com/folder/notebook3"
]

# Batch validation with error handling
all_results = []
batch_summary = {
    'total_notebooks': len(notebooks_to_validate),
    'successful_validations': 0,
    'failed_validations': 0,
    'total_passed': 0,
    'total_failed': 0
}

for i, notebook_path in enumerate(notebooks_to_validate):
    print(f"\n🔍 Validating {i+1}/{len(notebooks_to_validate)}: {notebook_path}")

    try:
        # Validate notebook
        results = validator.validate_notebook(notebook_path)
        summary = validator.get_validation_summary(results)

        # Update batch summary
        batch_summary['successful_validations'] += 1
        batch_summary['total_passed'] += summary['passed_results']
        batch_summary['total_failed'] += summary['failed_results']

        # Store results
        all_results.extend(results)

        print(f"   ✅ {summary['passed_results']} passed, ❌ {summary['failed_results']} failed")

    except Exception as e:
        print(f"   ❌ Error: {e}")
        batch_summary['failed_validations'] += 1

print(f"\n📊 Batch Validation Summary:")
print(f"   📚 Total notebooks: {batch_summary['total_notebooks']}")
print(f"   ✅ Successfully validated: {batch_summary['successful_validations']}")
print(f"   ❌ Failed to validate: {batch_summary['failed_validations']}")
print(f"   🎯 Total passed validations: {batch_summary['total_passed']}")
print(f"   🚫 Total failed validations: {batch_summary['total_failed']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Save Results to Delta Table

# COMMAND ----------

# Save validation results to Delta table for monitoring and reporting
if all_results:
    try:
        # Create DataFrame from all results
        results_df = validator.create_spark_dataframe(all_results, spark)

        # Add metadata columns
        from pyspark.sql.functions import current_timestamp, lit

        final_df = results_df.withColumn("validation_timestamp", current_timestamp()) \
                           .withColumn("validation_run_id", lit("manual_run_" + str(int(time.time())))) \
                           .withColumn("validation_source", lit("python_api"))

        # Replace with your actual table name
        table_name = "your_catalog.your_schema.validation_results"

        # Write to Delta table
        final_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(table_name)

        print(f"✅ Results saved to Delta table: {table_name}")
        print(f"📊 Saved {final_df.count()} validation records")

    except Exception as e:
        print(f"❌ Error saving to Delta table: {e}")
        print("Make sure you have write permissions to the target table/catalog")
else:
    print("⚠️ No results to save")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Advanced Usage Examples
# MAGIC
# MAGIC ### Custom Configuration
# MAGIC You can customize validation rules by modifying the `validation_rules.yaml` file or creating your own configuration.

# COMMAND ----------

# Example: Load custom validation configuration
custom_config_path = "/Workspace/Repos/your-username/databricks_code_validator/config/custom_validation_rules.yaml"

try:
    custom_config = YamlConfigManager(custom_config_path)
    print("✅ Custom configuration loaded")

    # Create validator with custom configuration
    custom_validator = DatabricksCodeValidator(
        llm_endpoint_url=llm_cfg.endpoint_url,
        llm_token=llm_cfg.api_key,
        workspace_client=workspace_client,
        config_manager=custom_config
    )

    print("✅ Custom validator created")

except FileNotFoundError:
    print("ℹ️ Custom configuration not found - using default configuration")
except Exception as e:
    print(f"❌ Error loading custom configuration: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting
# MAGIC
# MAGIC ### Common Issues:
# MAGIC
# MAGIC 1. **LLM Connection Issues**
# MAGIC    - Verify your API keys are correctly stored in Databricks Secrets
# MAGIC    - Check endpoint URLs are accessible from your cluster
# MAGIC    - Ensure your cluster has internet access for external LLM providers
# MAGIC
# MAGIC 2. **Notebook Path Issues**
# MAGIC    - Use workspace paths without `/Workspace` prefix
# MAGIC    - Ensure notebooks exist and are accessible
# MAGIC    - Check permissions on the notebooks
# MAGIC
# MAGIC 3. **Delta Table Issues**
# MAGIC    - Verify catalog and schema exist
# MAGIC    - Check write permissions
# MAGIC    - Ensure Unity Catalog is properly configured
# MAGIC
# MAGIC ### Debug Mode
# MAGIC For more detailed logging, you can enable debug mode:

# COMMAND ----------

# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Re-run validation with debug logging
debug_results = validator.validate_notebook("/Users/your.email@company.com/sample_notebook")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **Customize Validation Rules**: Edit `config/validation_rules.yaml` to match your organization's standards
# MAGIC 2. **Set Up Monitoring**: Create dashboards using the Delta table results
# MAGIC 3. **Integrate with Workflows**: Use this Python API in your data pipelines
# MAGIC 4. **Scale Up**: Consider **Option 2** (Scheduled Jobs) or **Option 3** (CI/CD Integration) for automated validation
# MAGIC
# MAGIC **Documentation**: See the README.md for complete deployment options and advanced configurations.