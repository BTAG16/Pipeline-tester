# Data Pipeline Project

## Overview
This repository contains a robust data pipeline implementation designed to extract, transform, and load (ETL) data from various sources. The pipeline is built with Python and includes features like logging, data validation, and multiple output formats.

## Project Structure
data-pipeline-project/
├── config/
│ └── config.yaml # Configuration file for data sources and paths
├── data/
│ ├── processed/ # Processed data outputs (CSV and Parquet)
│ └── raw/ # Raw extracted data
├── logs/ # Pipeline execution logs
├── requirements.txt # Python dependencies
├── src/
│ ├── init.py # Python package initialization
│ ├── pipeline.py # Main pipeline implementation
│ └── utils.py # Utility functions
├── tests/ # (Future) Test directory
└── venv/ # Python virtual environment

## Features

- **ETL Pipeline**: Complete Extract, Transform, Load workflow
- **Data Validation**: Checks for missing columns and empty datasets
- **Data Cleaning**: Handles duplicates, missing values, and column name standardization
- **Multiple Output Formats**: Saves data as both CSV (for readability) and Parquet (for performance)
- **Comprehensive Logging**: Tracks all pipeline operations with timestamps
- **Configuration Management**: Centralized YAML configuration for easy adjustments

## Installation

```bash
git clone https://github.com/BTAG16/Pipeline-tester.git
cd Pipeline-tester
python -m venv venv
source venv/bin/activate  # Linux/MacOS
pip install -r requirements.txt
```

Configuration
Edit config/config.yaml to:

Add/change data sources

Modify file paths

Adjust processing parameters

Example configuration:

data_sources:
  sample_sales: "https://raw.githubusercontent.com/BTAG16/Pipeline-tester/main/pipeline_analysis.csv"

paths:
  raw_data: "./data/raw"
  processed_data: "./data/processed"
  logs: "./logs"
Usage
Run the pipeline with default settings:

python src/pipeline.py
To specify a different data source (must be defined in config.yaml):

python
pipeline = DataPipeline()
result = pipeline.run_pipeline("your_source_name")
Outputs
The pipeline generates:

Raw data files in data/raw/

Processed data in both CSV and Parquet formats in data/processed/

Log files in logs/ with detailed execution information

Development
To extend or modify the pipeline:

Add new data sources to config.yaml

Implement additional transformation logic in transform_data() method

Add validation rules in validate_dataframe() function

Testing
The project includes a basic test file (config/sales_success_test.csv) for pipeline validation. Future test cases should be added to the tests/ directory.

Requirements
Python 3.8+

Packages listed in requirements.txt:

pandas

pyarrow

requests

PyYAML

License
MIT License

Author
BTAG16
