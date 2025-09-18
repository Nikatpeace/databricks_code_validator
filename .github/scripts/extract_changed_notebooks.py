#!/usr/bin/env python3
"""
Extract changed notebook files from PR for validation.
This script identifies notebooks that have changed in the current PR.
"""

import subprocess
import json
import os
from pathlib import Path
from typing import List, Dict


def get_changed_files() -> List[str]:
    """Get list of changed files in PR using git"""
    try:
        # Get files changed between base branch and current branch
        result = subprocess.run([
            'git', 'diff', '--name-only',
            'origin/main', 'HEAD'
        ], capture_output=True, text=True, check=True)

        changed_files = result.stdout.strip().split('\n')
        return [f for f in changed_files if f]  # Remove empty strings
    except subprocess.CalledProcessError as e:
        print(f"Error getting changed files: {e}")
        return []


def filter_notebook_files(files: List[str]) -> List[Dict[str, str]]:
    """Filter for notebook files (.ipynb, .py, .sql)"""
    notebook_extensions = {'.ipynb', '.py', '.sql'}
    notebook_files = []

    for file_path in files:
        path = Path(file_path)
        if path.suffix in notebook_extensions and path.exists():
            notebook_files.append({
                'path': file_path,
                'type': path.suffix,
                'size': path.stat().st_size,
                'workspace_path': convert_to_workspace_path(file_path)
            })

    return notebook_files


def convert_to_workspace_path(file_path: str) -> str:
    """Convert repository file path to Databricks workspace path"""
    # This assumes the repo is cloned to /Workspace/Repos/{org}/{repo}
    # Adjust this logic based on your repository structure

    if file_path.startswith('notebooks/'):
        # notebooks/my_notebook.py -> /Workspace/Repos/{org}/{repo}/notebooks/my_notebook
        workspace_path = f"/Workspace/Repos/{os.environ.get('GITHUB_REPOSITORY', 'org/repo')}/{file_path}"
        # Remove file extension for workspace path
        if workspace_path.endswith('.py'):
            workspace_path = workspace_path[:-3]
        elif workspace_path.endswith('.ipynb'):
            workspace_path = workspace_path[:-6]
        elif workspace_path.endswith('.sql'):
            workspace_path = workspace_path[:-4]
        return workspace_path
    else:
        # For files outside notebooks/, assume they map to a specific workspace location
        # This is environment-specific and should be customized
        return f"/Workspace/Repos/{os.environ.get('GITHUB_REPOSITORY', 'org/repo')}/{file_path}"


def read_notebook_content(file_path: str) -> Dict[str, str]:
    """Read notebook content"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        return {
            'local_path': file_path,
            'content': content,
            'size': len(content)
        }
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None


def main():
    """Main extraction logic"""
    print("🔍 Extracting changed notebooks from PR...")

    # Get changed files
    changed_files = get_changed_files()
    print(f"Found {len(changed_files)} changed files")

    # Filter for notebooks
    notebook_files = filter_notebook_files(changed_files)
    print(f"Found {len(notebook_files)} notebook files")

    if not notebook_files:
        print("✅ No notebooks changed in this PR")
        # Set GitHub Actions output
        with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
            f.write("notebook_count=0\n")
            f.write("has_notebooks=false\n")
        return

    # Read notebook contents
    notebooks_with_content = []
    for nb_info in notebook_files:
        content_info = read_notebook_content(nb_info['path'])
        if content_info:
            # Combine file info with content
            full_info = {**nb_info, **content_info}
            notebooks_with_content.append(full_info)

    # Save to file for next step
    output_file = 'changed_notebooks.json'
    with open(output_file, 'w') as f:
        json.dump(notebooks_with_content, f, indent=2)

    print(f"📝 Prepared {len(notebooks_with_content)} notebooks for validation")

    # Log notebook paths for visibility
    for nb in notebooks_with_content:
        print(f"  📓 {nb['local_path']} -> {nb['workspace_path']}")

    # Set GitHub Actions output
    with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
        f.write(f"notebook_count={len(notebooks_with_content)}\n")
        f.write(f"has_notebooks=true\n")
        f.write(f"notebooks_file={output_file}\n")


if __name__ == "__main__":
    main()