#!/usr/bin/env python3
"""
Command-line interface for the Databricks Code Validator.

This CLI makes the bot easy to use as a solution accelerator.
"""

import argparse
import sys
import os
import json
from pathlib import Path
from typing import List, Optional
import base64

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat

from .main import DatabricksCodeValidator
from .config.yaml_config import YamlConfigManager, create_default_config
from .config.llm_providers import get_llm_provider, get_provider_examples
from .utils.logging_utils import setup_logging
from .utils.spark_utils import get_or_create_spark_session


def is_notebook_environment():
    """Check if we're running in a Databricks notebook environment."""
    
    # Primary check: Look for Databricks environment variables
    databricks_env_vars = [
        'DATABRICKS_RUNTIME_VERSION',
        'DATABRICKS_CLUSTER_ID', 
        'DB_CLUSTER_ID',
        'SPARK_LOCAL_IP'
    ]
    
    has_databricks_env = any(os.environ.get(var) for var in databricks_env_vars)
    
    # If we have Databricks env vars, we're definitely in a Databricks environment
    if has_databricks_env:
        if os.environ.get('DEBUG_AUTH'):
            print(f"DEBUG DETECTION: Found Databricks env vars: {[var for var in databricks_env_vars if os.environ.get(var)]}")
        return True
    
    # Secondary checks for other indicators
    try:
        # Check for IPython (notebook/interactive environment)
        import IPython
        ip = IPython.get_ipython()
        has_ipython = ip is not None
        
        # Check for Spark context
        try:
            from pyspark import SparkContext
            spark_available = SparkContext._active_spark_context is not None
        except:
            spark_available = False
        
        result = has_ipython or spark_available
        
        if os.environ.get('DEBUG_AUTH'):
            print(f"DEBUG DETECTION: has_databricks_env={has_databricks_env}, has_ipython={has_ipython}, spark_available={spark_available}")
            print(f"DEBUG DETECTION: final result={result}")
        
        return result
        
    except ImportError:
        if os.environ.get('DEBUG_AUTH'):
            print("DEBUG DETECTION: IPython not available, assuming not notebook")
        return False
    except Exception as e:
        if os.environ.get('DEBUG_AUTH'):
            print(f"DEBUG DETECTION: Exception occurred: {e}, assuming notebook=True")
        return True


def create_workspace_client(args):
    """Create a WorkspaceClient with proper authentication handling."""
    
    # Check if we're in a notebook environment
    in_notebook = is_notebook_environment()
    
    # Debug output to help diagnose issues
    if os.environ.get('DEBUG_AUTH'):
        print(f"DEBUG: Notebook environment detected: {in_notebook}")
        print(f"DEBUG: Has IPython: {'IPython' in sys.modules}")
        print(f"DEBUG: Databricks env vars: {[var for var in ['DATABRICKS_RUNTIME_VERSION', 'DATABRICKS_CLUSTER_ID', 'DB_CLUSTER_ID', 'SPARK_LOCAL_IP'] if os.environ.get(var)]}")
        print(f"DEBUG: CLI args - host: {bool(getattr(args, 'databricks_host', None))}, token: {bool(getattr(args, 'databricks_token', None))}")
        print(f"DEBUG: Env vars - host: {bool(os.environ.get('DATABRICKS_HOST'))}, token: {bool(os.environ.get('DATABRICKS_TOKEN'))}")
    
    # Try explicit authentication first
    if args.databricks_host and args.databricks_token:
        try:
            return WorkspaceClient(host=args.databricks_host, token=args.databricks_token)
        except Exception as e:
            print(f"Error with explicit authentication: {e}")
            sys.exit(1)
    
    # Try environment variables
    if os.environ.get('DATABRICKS_HOST') and os.environ.get('DATABRICKS_TOKEN'):
        try:
            return WorkspaceClient(
                host=os.environ.get('DATABRICKS_HOST'),
                token=os.environ.get('DATABRICKS_TOKEN')
            )
        except Exception as e:
            print(f"Error with environment variable authentication: {e}")
            sys.exit(1)
    
    # For notebook environments, avoid default auth that causes IPython issues
    if in_notebook:
        print("Error: Running in Databricks notebook environment requires explicit authentication.")
        print("\nIn Databricks notebooks, you must provide authentication explicitly:")
        print("Option 1 - Use CLI arguments:")
        print("  !databricks-code-validator validate \\")
        print("    --databricks-host 'https://your-workspace.cloud.databricks.com' \\")
        print("    --databricks-token 'dapi-your-token' \\")
        print("    --notebook '/path/to/notebook'")
        print("\nOption 2 - Set environment variables in your notebook:")
        print("  import os")
        print("  os.environ['DATABRICKS_HOST'] = 'https://your-workspace.cloud.databricks.com'")
        print("  os.environ['DATABRICKS_TOKEN'] = 'dapi-your-token'")
        print("  !databricks-code-validator validate --notebook '/path/to/notebook'")
        print("\nTo get your token: Databricks Settings -> Developer -> Access Tokens")
        sys.exit(1)
    
    # Try default authentication only outside notebook environments
    try:
        return WorkspaceClient()
    except Exception as e:
        print(f"Error creating Databricks workspace client: {e}")
        print("\nAuthentication options:")
        print("1. Use CLI arguments: --databricks-host <url> --databricks-token <token>")
        print("2. Set environment variables: DATABRICKS_HOST and DATABRICKS_TOKEN")
        print("3. Use Databricks CLI configuration: databricks configure --token")
        sys.exit(1)


def create_config_command(args):
    """Create a new configuration file."""
    config_manager = YamlConfigManager()
    
    if args.non_interactive:
        # Use defaults without prompting
        config = create_default_config()
    else:
        if args.template:
            # Create from template
            if args.template == "default":
                config = create_default_config()
            else:
                print(f"Unknown template: {args.template}")
                sys.exit(1)
        else:
            # Create minimal config
            config = create_default_config()
    
    output_path = args.output or "validation_rules.yaml"
    config_manager.config = config
    config_manager.save_config(output_path)
    
    print(f"Configuration file created: {output_path}")
    print("Edit this file to customize your validation rules.")


def validate_command(args):
    """Validate notebook(s) against standards."""
    # Setup logging
    setup_logging(log_level=args.log_level.upper())
    
    # Load configuration
    config_manager = YamlConfigManager(args.config)
    
    try:
        validation_config = config_manager.load_config()
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Create a configuration file using: databricks-code-validator create-config")
        sys.exit(1)
    
    # Get LLM provider configuration
    try:
        llm_config = get_llm_provider()
    except ValueError as e:
        print(f"Error: {e}")
        print("Set the required environment variables or use --help for examples")
        sys.exit(1)
    
    # Initialize WorkspaceClient with proper authentication
    print("Initializing Databricks workspace client...")
    w = create_workspace_client(args)
    
    # Initialize the bot
    print("Initializing Databricks Code Validator...")
    bot = DatabricksCodeValidator(
        llm_endpoint_url=llm_config.endpoint_url,
        llm_token=llm_config.api_key,
        workspace_client=w,
        config_manager=config_manager
    )
    
    # Determine notebooks to validate
    notebooks = []
    
    if args.notebook:
        export = w.workspace.export(path=args.notebook, format=ExportFormat.JUPYTER)
        notebooks.append({'path': args.notebook, 'content': base64.b64decode(export.content).decode('utf-8')})
    elif args.notebooks_file:
        with open(args.notebooks_file, 'r') as f:
            paths = [line.strip() for line in f if line.strip()]
        for path in paths:
            export = w.workspace.export(path=path, format=ExportFormat.JUPYTER)
            notebooks.append({'path': path, 'content': base64.b64decode(export.content).decode('utf-8')})
    elif args.notebooks:
        for path in args.notebooks:
            export = w.workspace.export(path=path, format=ExportFormat.JUPYTER)
            notebooks.append({'path': path, 'content': base64.b64decode(export.content).decode('utf-8')})
    else:
        print("Error: No notebooks specified for validation")
        sys.exit(1)
    
    # Validate notebooks
    all_results = []
    for notebook in notebooks:
        print(f"\nValidating: {notebook['path']}")
        try:
            # Assuming validate_notebook can be modified to take content; if not, adjust bot
            results = bot.validate_notebook(notebook['path'], content=notebook['content'])  # May need bot update
            all_results.extend(results)
            
            # Show summary for this notebook
            summary = bot.get_validation_summary(results)
            print(f"  Results: {summary['passed_results']} passed, "
                  f"{summary['failed_results']} failed, "
                  f"{summary['pending_results']} pending")
            
        except Exception as e:
            print(f"  Error validating {notebook['path']}: {e}")
            continue
    
    # Overall summary
    if all_results:
        overall_summary = bot.get_validation_summary(all_results)
        print(f"\n=== Overall Summary ===")
        print(f"Total results: {overall_summary['total_results']}")
        print(f"Passed: {overall_summary['passed_results']}")
        print(f"Failed: {overall_summary['failed_results']}")
        print(f"Pending: {overall_summary['pending_results']}")
        print(f"Pass rate: {overall_summary['pass_rate']:.2%}")
        
        # Save results
        if args.output:
            save_results(all_results, args.output, args.format)
        
        # Save to Spark/Delta if requested
        if args.save_to_table:
            save_to_spark_table(bot, all_results, args.save_to_table)
        
        # Exit with error code if any failures
        if overall_summary['failed_results'] > 0 and args.fail_on_errors:
            sys.exit(1)
    else:
        print("No results to display")
        sys.exit(1)


def save_results(results, output_path: str, format: str):
    """Save validation results to file."""
    output_path_obj = Path(output_path)
    
    if format == "json":
        with open(output_path_obj, 'w') as f:
            result_dicts = [r.to_dict() for r in results]
            json.dump(result_dicts, f, indent=2, default=str)
    elif format == "csv":
        import csv
        with open(output_path_obj, 'w', newline='') as f:
            if results:
                writer = csv.DictWriter(f, fieldnames=results[0].to_dict().keys())
                writer.writeheader()
                for result in results:
                    writer.writerow(result.to_dict())
    elif format == "html":
        generate_html_report(results, output_path_obj)
    
    print(f"Results saved to: {output_path}")


def generate_html_report(results, output_path: Path):
    """Generate an HTML report."""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Databricks Code Validator Report</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .summary { background: #f5f5f5; padding: 15px; border-radius: 5px; margin-bottom: 20px; }
            .passed { color: green; }
            .failed { color: red; }
            .pending { color: orange; }
            table { border-collapse: collapse; width: 100%; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #f2f2f2; }
        </style>
    </head>
    <body>
        <h1>Databricks Code Validation Report</h1>
        <div class="summary">
            <h2>Summary</h2>
            <p>Total validations: {total}</p>
            <p class="passed">Passed: {passed}</p>
            <p class="failed">Failed: {failed}</p>
            <p class="pending">Pending: {pending}</p>
        </div>
        <h2>Detailed Results</h2>
        <table>
            <tr>
                <th>Notebook</th>
                <th>Rule</th>
                <th>Status</th>
                <th>Details</th>
            </tr>
            {rows}
        </table>
    </body>
    </html>
    """
    
    # Calculate summary
    total = len(results)
    passed = sum(1 for r in results if r.status.value == "Passed")
    failed = sum(1 for r in results if r.status.value == "Failed")
    pending = sum(1 for r in results if r.status.value == "Pending")
    
    # Generate table rows
    rows = []
    for result in results:
        status_class = result.status.value.lower()
        row = f"""
        <tr>
            <td>{result.notebook_path}</td>
            <td>{result.rule}</td>
            <td class="{status_class}">{result.status.value}</td>
            <td>{result.details}</td>
        </tr>
        """
        rows.append(row)
    
    html = html_content.format(
        total=total,
        passed=passed,
        failed=failed,
        pending=pending,
        rows=''.join(rows)
    )
    
    with open(output_path, 'w') as f:
        f.write(html)


def save_to_spark_table(bot, results, table_name: str):
    """Save results to Spark/Delta table."""
    try:
        spark = get_or_create_spark_session()
        if spark:
            df = bot.create_spark_dataframe(results, spark)
            df.write.format("delta").mode("overwrite").saveAsTable(table_name)
            print(f"Results saved to Delta table: {table_name}")
        else:
            print("Warning: Spark not available, skipping table save")
    except Exception as e:
        print(f"Error saving to table: {e}")


def show_examples_command(args):
    """Show configuration examples."""
    if args.type == "llm":
        examples = get_provider_examples()
        print("LLM Provider Configuration Examples:")
        print("=====================================")
        for provider, config in examples.items():
            print(f"\n{provider.upper()}:")
            print(f"Environment variables:")
            print(f"  export LLM_PROVIDER_TYPE={provider}")
            print(f"  export LLM_API_KEY={config['api_key']}")
            if 'endpoint_url' in config:
                print(f"  export LLM_ENDPOINT_URL={config['endpoint_url']}")
            print(f"  export LLM_MODEL_NAME={config['model_name']}")
    elif args.type == "config":
        print("Example validation_rules.yaml configuration:")
        print("==========================================")
        print("Use 'databricks-code-validator create-config' to generate a full example")
    else:
        print("Available example types: llm, config")


def main(**kwargs):
    """Main CLI entry point.

    Args:
        **kwargs: Optional keyword arguments from Databricks job parameters
    """
    # If called with kwargs (from Databricks job), convert to sys.argv format
    if kwargs:
        # Convert job parameters to command line arguments
        sys.argv = ['databricks-code-validator']

        # Add command (required)
        command = kwargs.get('command', 'validate')
        sys.argv.append(command)

        # Convert other parameters to CLI format
        for key, value in kwargs.items():
            if key == 'command':
                continue  # Already added

            # Convert underscores to dashes for CLI compatibility
            cli_key = key.replace('_', '-')

            if isinstance(value, bool) and value:
                sys.argv.append(f'--{cli_key}')
            elif value is not None and value != '':
                sys.argv.extend([f'--{cli_key}', str(value)])

    parser = argparse.ArgumentParser(
        description="Databricks Code Validator - AI-powered validation of Databricks notebooks against code standards and best practices",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create a configuration file
  databricks-code-validator create-config --output my_rules.yaml

  # Validate a single notebook
  databricks-code-validator validate --notebook "/path/to/notebook" --config my_rules.yaml

  # Validate multiple notebooks
  databricks-code-validator validate --notebooks "/path/1" "/path/2" --output results.json

  # Show LLM provider examples
  databricks-code-validator examples --type llm

Environment variables:
  LLM_PROVIDER_TYPE     Type of LLM provider (openai, azure_openai, anthropic, databricks)
  LLM_API_KEY          API key for the LLM service
  LLM_ENDPOINT_URL     LLM endpoint URL (provider-specific)
  LLM_MODEL_NAME       Model name to use
  DATABRICKS_HOST      Databricks workspace URL (for authentication)
  DATABRICKS_TOKEN     Databricks personal access token (for authentication)
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Create config command
    config_parser = subparsers.add_parser('create-config', help='Create a new configuration file')
    config_parser.add_argument('--output', '-o', help='Output file path (default: validation_rules.yaml)')
    config_parser.add_argument('--template', help='Template to use (default)')
    config_parser.add_argument('--non-interactive', action='store_true', help='Run without interactive prompts')
    config_parser.set_defaults(func=create_config_command)
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate notebooks')
    validate_parser.add_argument('--notebook', help='Single notebook path to validate')
    validate_parser.add_argument('--notebooks', nargs='+', help='Multiple notebook paths to validate')
    validate_parser.add_argument('--notebooks-file', help='File containing list of notebook paths')
    validate_parser.add_argument('--config', '-c', default='validation_rules.yaml', 
                                help='Path to configuration file (default: validation_rules.yaml)')
    validate_parser.add_argument('--output', '-o', help='Output file for results')
    validate_parser.add_argument('--format', choices=['json', 'csv', 'html'], default='json',
                                help='Output format (default: json)')
    validate_parser.add_argument('--save-to-table', help='Save results to Spark/Delta table')
    validate_parser.add_argument('--fail-on-errors', action='store_true',
                                help='Exit with error code if any validations fail')
    validate_parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                                default='INFO', help='Logging level (default: INFO)')
    validate_parser.add_argument('--non-interactive', action='store_true', help='Run without interactive prompts')
    # Authentication options
    validate_parser.add_argument('--databricks-host', help='Databricks workspace URL (e.g., https://your-workspace.cloud.databricks.com)')
    validate_parser.add_argument('--databricks-token', help='Databricks personal access token')
    validate_parser.set_defaults(func=validate_command)
    
    # Examples command
    examples_parser = subparsers.add_parser('examples', help='Show configuration examples')
    examples_parser.add_argument('--type', choices=['llm', 'config'], required=True,
                                help='Type of examples to show')
    examples_parser.set_defaults(func=show_examples_command)
    
    # Parse arguments
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Execute command
    args.func(args)


if __name__ == '__main__':
    main() 