"""
Setup script for Dimensional Processing Library.
"""

from setuptools import setup, find_packages
import os

# Read the README file
def read_readme():
    with open("README.md", "r", encoding="utf-8") as fh:
        return fh.read()

# Read requirements
def read_requirements():
    with open("requirements.txt", "r", encoding="utf-8") as fh:
        return [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="dimensional-processing",
    version="1.0.0",
    author="Data Engineering Team",
    author_email="data-engineering@company.com",
    description="A production-ready library for implementing SCD Type 2 dimensional tables and dimensional key resolution in Databricks environments",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/company/data-accel",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    python_requires=">=3.8",
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=6.0.0",
            "pytest-cov>=2.12.0",
            "pytest-mock>=3.6.0",
            "flake8>=3.9.0",
            "black>=21.0.0",
            "mypy>=0.910",
            "sphinx>=4.0.0",
            "sphinx-rtd-theme>=0.5.0",
        ],
        "docs": [
            "sphinx>=4.0.0",
            "sphinx-rtd-theme>=0.5.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "dimensional-processing=dimensional_processing.cli:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
    keywords="databricks, spark, scd, dimensional, data-engineering, etl",
    project_urls={
        "Bug Reports": "https://github.com/company/data-accel/issues",
        "Source": "https://github.com/company/data-accel",
        "Documentation": "https://github.com/company/data-accel/docs",
    },
)
