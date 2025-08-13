# src/utils.py
import logging
import os
import yaml
from datetime import datetime
import pandas as pd

def setup_logging(log_path="./logs"):
    """Setup logging configuration"""
    os.makedirs(log_path, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"{log_path}/pipeline_{datetime.now().strftime('%Y%m%d')}.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_config(config_path="./config/config.yaml"):
	"""Load configuration from YAML file"""
	with open(config_path, "r") as file:
		return yaml.safe_load(file)

def validate_dataframe(df, required_columns=None):
	"""Basic data validation"""
	if df.empty:
		raise ValueError("DataFrame is Empty")

	if required_columns:
		missing_cols = set(required_columns) - set(df.columns)
		if missing_cols:
			raise ValueError("Missing required columns: {missing_cols}")

	return True

def clean_column_names(df):
	"""Cleaned Columns"""
	df.columns = df.columns.str.lower().str.replace(' ','_').str.replace(r'[^a-zA-Z0-9_]', '', regex=True)
	return df
