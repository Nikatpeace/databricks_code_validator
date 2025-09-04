#!/usr/bin/env python3
"""Setup script for Databricks Code Validator."""

from setuptools import setup, find_packages

# Read the README file
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read requirements
with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="databricks-code-validator",
    version="1.0.0",
    author="Your Organization",
    author_email="your-email@company.com",
    description="AI-powered validation tool for Databricks notebooks against code standards and best practices",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/databricks-code-validator",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "databricks-code-validator=databricks_code_validator.cli:main",
        ],
    },
    include_package_data=True,
    package_data={
        "databricks_code_validator": ["config/*.yaml"],
    },
    keywords="databricks, code-quality, ai, validation, standards, notebooks",
)