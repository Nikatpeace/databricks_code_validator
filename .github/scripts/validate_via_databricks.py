#!/usr/bin/env python3
"""
Validate notebooks using Databricks APIs.
This script sends notebooks to Databricks for validation without using DBFS.
"""

import json
import os
import sys
import time
import tempfile
from pathlib import Path
from typing import List, Dict, Any
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import CreateJob, JobTaskSettings, NotebookTask, JobCluster, NewCluster


class DatabricksAPIValidator:
    def __init__(self):
        self.workspace_client = WorkspaceClient()
        self.validation_repo_path = f"/Workspace/Repos/{os.environ.get('GITHUB_REPOSITORY', 'org/repo')}"

    def validate_notebooks_via_job(self, notebooks_data: List[Dict], pr_metadata: Dict) -> Dict:
        """
        Submit a validation job to Databricks that validates notebooks.
        This uses a temporary notebook approach instead of DBFS.
        """

        # Create temporary validation notebook content
        validation_notebook_content = self.create_validation_notebook(notebooks_data, pr_metadata)

        # Upload temporary notebook to workspace
        temp_notebook_path = f"/tmp/ci_validation_pr_{pr_metadata['pr_number']}"
        self.upload_temp_notebook(temp_notebook_path, validation_notebook_content)

        try:
            # Submit job that runs the temporary notebook
            job_run = self.workspace_client.jobs.submit(
                run_name=f"CI Validation - PR #{pr_metadata['pr_number']}",
                tasks=[
                    JobTaskSettings(
                        task_key="validate_notebooks",
                        notebook_task=NotebookTask(
                            notebook_path=temp_notebook_path
                        ),
                        job_cluster_key="validation_cluster"
                    )
                ],
                job_clusters=[
                    JobCluster(
                        job_cluster_key="validation_cluster",
                        new_cluster=NewCluster(
                            spark_version="13.3.x-scala2.12",
                            node_type_id="i3.xlarge",
                            num_workers=1,
                            libraries=[],  # Assume wheel is pre-installed or install via %pip
                            spark_conf={
                                "spark.sql.adaptive.enabled": "true"
                            }
                        )
                    )
                ]
            )

            print(f"🚀 Submitted validation job: {job_run.run_id}")
            return self.wait_for_job_completion(job_run.run_id)

        finally:
            # Cleanup temporary notebook
            self.cleanup_temp_notebook(temp_notebook_path)

    def create_validation_notebook(self, notebooks_data: List[Dict], pr_metadata: Dict) -> str:
        """Create a Databricks notebook that validates the provided notebooks"""

        # Convert notebooks data to a JSON string for embedding
        notebooks_json = json.dumps(notebooks_data, indent=2)

        notebook_content = f'''# Databricks notebook source
# MAGIC %md
# MAGIC # CI/CD Validation Notebook - PR #{pr_metadata['pr_number']}
# MAGIC
# MAGIC This notebook validates notebooks from PR #{pr_metadata['pr_number']} ({pr_metadata['pr_branch']})

# COMMAND ----------

# Install the validator wheel
%pip install git+https://github.com/{os.environ.get('GITHUB_REPOSITORY', 'org/repo')}.git@{pr_metadata['github_sha']}
dbutils.library.restartPython()

# COMMAND ----------

import json
import os
from datetime import datetime

# Set up environment
os.environ['LLM_PROVIDER_TYPE'] = '{os.environ.get('LLM_PROVIDER_TYPE', 'databricks')}'
os.environ['LLM_ENDPOINT_URL'] = dbutils.secrets.get("llm_credentials", "endpoint_url")
os.environ['LLM_API_KEY'] = dbutils.secrets.get("llm_credentials", "api_key")
os.environ['LLM_MODEL_NAME'] = '{os.environ.get('LLM_MODEL_NAME', 'databricks-dbrx-instruct')}'

print("✅ Environment configured")

# COMMAND ----------

# Initialize validator
from databricks_code_validator.main import DatabricksCodeValidator
from databricks_code_validator.config.yaml_config import YamlConfigManager
from databricks_code_validator.config.llm_providers import get_llm_provider
from databricks.sdk import WorkspaceClient

config_path = "{os.environ.get('VALIDATION_CONFIG_PATH', '/Workspace/Repos/org/repo/config/validation_rules.yaml')}"
config = YamlConfigManager(config_path)

llm_cfg = get_llm_provider()
workspace_client = WorkspaceClient()

validator = DatabricksCodeValidator(
    llm_endpoint_url=llm_cfg.endpoint_url,
    llm_token=llm_cfg.api_key,
    workspace_client=workspace_client,
    config_manager=config
)

print("✅ Validator initialized")

# COMMAND ----------

# Embedded notebooks data from PR
notebooks_data = {notebooks_json}

print(f"📚 Validating {{len(notebooks_data)}} notebooks from PR")

# COMMAND ----------

# Validate each notebook
all_results = []
validation_summary = {{
    'pr_number': {pr_metadata['pr_number']},
    'pr_branch': '{pr_metadata['pr_branch']}',
    'github_sha': '{pr_metadata['github_sha']}',
    'timestamp': datetime.now().isoformat(),
    'total_notebooks': len(notebooks_data),
    'passed_results': 0,
    'failed_results': 0,
    'notebook_results': []
}}

for i, notebook_info in enumerate(notebooks_data):
    notebook_path = notebook_info['workspace_path']
    print(f"🔍 Validating {{i+1}}/{{len(notebooks_data)}}: {{notebook_path}}")

    try:
        # Validate the notebook using workspace path
        results = validator.validate_notebook(notebook_path)
        summary = validator.get_validation_summary(results)

        validation_summary['passed_results'] += summary['passed_results']
        validation_summary['failed_results'] += summary['failed_results']

        all_results.extend(results)

        notebook_result = {{
            'notebook_path': notebook_path,
            'local_path': notebook_info['local_path'],
            'passed': summary['passed_results'],
            'failed': summary['failed_results']
        }}
        validation_summary['notebook_results'].append(notebook_result)

        print(f"   ✅ {{summary['passed_results']}} passed, ❌ {{summary['failed_results']}} failed")

    except Exception as e:
        print(f"   ❌ Error: {{e}}")
        validation_summary['failed_results'] += 1

        notebook_result = {{
            'notebook_path': notebook_path,
            'local_path': notebook_info['local_path'],
            'passed': 0,
            'failed': 1,
            'error': str(e)
        }}
        validation_summary['notebook_results'].append(notebook_result)

print(f"\\n📊 Validation Summary:")
print(f"   Passed: {{validation_summary['passed_results']}}")
print(f"   Failed: {{validation_summary['failed_results']}}")

# COMMAND ----------

# Save results to Delta table
if all_results:
    try:
        from pyspark.sql.functions import lit, current_timestamp

        df = validator.create_spark_dataframe(all_results, spark)

        enriched_df = df.withColumn("pr_number", lit({pr_metadata['pr_number']})) \\
                       .withColumn("pr_branch", lit("{pr_metadata['pr_branch']}")) \\
                       .withColumn("github_sha", lit("{pr_metadata['github_sha']}")) \\
                       .withColumn("validation_source", lit("ci_cd")) \\
                       .withColumn("validation_timestamp", current_timestamp())

        # Write to Delta table (customize table name as needed)
        table_name = "main.governance.ci_validation_results"
        enriched_df.write \\
            .format("delta") \\
            .mode("append") \\
            .option("mergeSchema", "true") \\
            .saveAsTable(table_name)

        print(f"✅ Results saved to Delta table: {{table_name}}")

    except Exception as e:
        print(f"❌ Error saving to Delta: {{e}}")

# COMMAND ----------

# Output results for GitHub Actions
print(f"VALIDATION_SUMMARY:{{json.dumps(validation_summary)}}")

# Exit with error if validations failed
if validation_summary['failed_results'] > 0:
    dbutils.notebook.exit("FAILED")
else:
    dbutils.notebook.exit("SUCCESS")
'''

        return notebook_content

    def upload_temp_notebook(self, notebook_path: str, content: str):
        """Upload temporary notebook to Databricks workspace"""
        try:
            # Convert content to base64 for upload
            import base64
            content_b64 = base64.b64encode(content.encode()).decode()

            self.workspace_client.workspace.import_(
                path=notebook_path,
                format="SOURCE",
                content=content_b64,
                overwrite=True
            )
            print(f"📁 Uploaded temporary notebook to {notebook_path}")

        except Exception as e:
            print(f"❌ Error uploading notebook: {e}")
            raise

    def cleanup_temp_notebook(self, notebook_path: str):
        """Clean up temporary notebook"""
        try:
            self.workspace_client.workspace.delete(notebook_path)
            print(f"🗑️ Cleaned up temporary notebook: {notebook_path}")
        except Exception as e:
            print(f"⚠️ Warning: Could not cleanup {notebook_path}: {e}")

    def wait_for_job_completion(self, run_id: str, timeout_minutes: int = 10) -> Dict:
        """Wait for job completion and return results"""

        timeout_seconds = timeout_minutes * 60
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            run_info = self.workspace_client.jobs.get_run(run_id)

            if run_info.state.life_cycle_state in ['TERMINATED', 'SKIPPED']:
                print(f"✅ Job completed: {run_info.state.result_state}")

                # Get job output
                try:
                    run_output = self.workspace_client.jobs.get_run_output(run_id)
                    return self.parse_validation_results(run_output)
                except Exception as e:
                    print(f"Warning: Could not get detailed results: {e}")
                    return {
                        "status": run_info.state.result_state,
                        "passed_results": 0,
                        "failed_results": 1 if run_info.state.result_state == "FAILED" else 0
                    }

            elif run_info.state.life_cycle_state in ['INTERNAL_ERROR']:
                print(f"❌ Job failed: {run_info.state.state_message}")
                return {
                    "status": "FAILED",
                    "passed_results": 0,
                    "failed_results": 1,
                    "error": run_info.state.state_message
                }

            print(f"⏳ Job running... ({run_info.state.life_cycle_state})")
            time.sleep(30)

        print(f"⏰ Job timed out after {timeout_minutes} minutes")
        return {
            "status": "TIMEOUT",
            "passed_results": 0,
            "failed_results": 1,
            "error": "Job execution timed out"
        }

    def parse_validation_results(self, run_output) -> Dict:
        """Parse validation results from job output"""

        try:
            logs = run_output.logs if run_output.logs else ""

            # Look for validation summary in logs
            if "VALIDATION_SUMMARY:" in logs:
                summary_line = logs.split("VALIDATION_SUMMARY:")[1].split("\\n")[0]
                return json.loads(summary_line)

        except Exception as e:
            print(f"Error parsing results: {e}")

        # Fallback result
        return {
            "status": "UNKNOWN",
            "passed_results": 0,
            "failed_results": 0,
            "message": "Could not parse validation results from job output"
        }


def main():
    """Main validation orchestration"""

    print("🔄 Starting Databricks API validation...")

    # Check if we have notebooks to validate
    try:
        with open('changed_notebooks.json', 'r') as f:
            notebooks_data = json.load(f)
    except FileNotFoundError:
        print("ℹ️ No changed_notebooks.json found - no notebooks to validate")
        sys.exit(0)

    if not notebooks_data:
        print("✅ No notebooks to validate")
        sys.exit(0)

    # Prepare PR metadata
    pr_metadata = {
        'pr_number': os.environ['PR_NUMBER'],
        'pr_branch': os.environ['PR_BRANCH'],
        'github_sha': os.environ['GITHUB_SHA']
    }

    print(f"📋 Validating {len(notebooks_data)} notebooks for PR #{pr_metadata['pr_number']}")

    # Initialize validator and submit job
    validator = DatabricksAPIValidator()

    try:
        # Submit validation job
        results = validator.validate_notebooks_via_job(notebooks_data, pr_metadata)

        print(f"📊 Validation Results:")
        print(f"   Status: {results.get('status', 'UNKNOWN')}")
        print(f"   Passed: {results.get('passed_results', 0)}")
        print(f"   Failed: {results.get('failed_results', 0)}")

        # Determine CI success/failure
        if results.get('failed_results', 0) > 0 or results.get('status') in ['FAILED', 'TIMEOUT']:
            print("❌ Validation failed - blocking PR")
            sys.exit(1)
        else:
            print("✅ All validations passed")
            sys.exit(0)

    except Exception as e:
        print(f"❌ Validation error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()