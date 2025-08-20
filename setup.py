#!/usr/bin/env python3
"""Setup script for Code Standards Bot."""

from setuptools import setup, find_packages

# Read the README file
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read requirements
with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="code-standards-bot",
    version="1.0.0",
    author="Your Organization",
    author_email="your-email@company.com",
    description="AI-powered validation of Databricks notebooks against code standards",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/code-standards-bot",
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
            "code-standards-bot=code_standards_bot.cli:main",
        ],
    },
    include_package_data=True,
    package_data={
        "code_standards_bot": ["config/*.yaml"],
    },
    keywords="databricks, code-quality, ai, validation, standards, notebooks",
)