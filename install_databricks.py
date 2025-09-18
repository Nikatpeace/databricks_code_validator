#!/usr/bin/env python3
"""
Installation script for Databricks Code Validator in Databricks environments.

This script provides a robust installation method that works around
common issues with editable installs in Databricks workspace environments.
"""

import subprocess
import sys
import os


def install_package():
    """Install the databricks-code-validator package."""

    # Get the current directory (should be the repo root)
    repo_path = os.getcwd()
    print(f"Installing from: {repo_path}")

    try:
        # Method 1: Try editable install
        print("Attempting editable install...")
        result = subprocess.run([
            sys.executable, "-m", "pip", "install", "-e", "."
        ], cwd=repo_path, capture_output=True, text=True, check=True)

        print("✅ Editable install successful!")
        print("You can now use:")
        print("  from dbx_code_validator.main import DatabricksCodeValidator")

    except subprocess.CalledProcessError as e:
        print("❌ Editable install failed:")
        print(f"Error: {e.stderr}")

        # Method 2: Fallback to direct path method
        print("\n🔄 Using fallback method...")
        print("Add this code to your notebook:")
        print(f"""
import sys
sys.path.insert(0, '{repo_path}')
from dbx_code_validator.main import DatabricksCodeValidator
""")


def test_import():
    """Test if the package can be imported."""
    try:
        from dbx_code_validator.main import DatabricksCodeValidator
        print("✅ Import test successful!")
        return True
    except ImportError as e:
        print(f"❌ Import test failed: {e}")
        return False


if __name__ == "__main__":
    print("🚀 Databricks Code Validator Installation")
    print("=" * 50)

    install_package()

    print("\n" + "=" * 50)
    print("🧪 Testing installation...")

    if test_import():
        print("\n✅ Installation complete! Package is ready to use.")
    else:
        print("\n⚠️ Installation completed but import test failed.")
        print("Please use the fallback method shown above.")