# src/pipeline.py
import pandas as pd
import requests
import os
from datetime import datetime
import pyarrow.parquet as pq
from src.utils import setup_logging, load_config, validate_dataframe, clean_column_names

class DataPipeline:
	def __init__(self, config_path="./config/config.yaml"):
			self.config = load_config(config_path)
			self.logger = setup_logging(self.config['paths']['logs'])
			self.logger.info("Data Pipeline initialized")

	def extract_data(self, source_name):
		"""Extract data from source"""
		try:
			url = self.config['data_sources'][source_name]
			self.logger.info(f"Extracting data from {url}")

			response = requests.get(url)
			response.raise_for_status()  # Raises exception for HTTP errors

			# Save raw data (crucial for debugging and reprocessing)
			raw_path = os.path.join(self.config['paths']['raw_data'], f"{source_name}_{datetime.now().strftime('%Y%m%d')}.csv")
			os.makedirs(os.path.dirname(raw_path), exist_ok=True)

			with open(raw_path, 'w') as f:
				f.write(response.text)

			# Load into DataFrame
			df = pd.read_csv(raw_path)
			self.logger.info(f"Extracted {len(df)} rows from {source_name}")
			return df

		except Exception as e:
				self.logger.error(f"Error extracting data: {str(e)}")
				raise

	def transform_data(self, df):
		"""Transform the data"""
		try:
				self.logger.info("Starting data transformation")

				# Clean column names
				df = clean_column_names(df)

				# Remove duplicates
				initial_rows = len(df)
				df = df.drop_duplicates()
				self.logger.info(f"Removed {initial_rows - len(df)} duplicate rows")

				# Handle missing values
				numeric_columns = df.select_dtypes(include=['number']).columns
				df[numeric_columns] = df[numeric_columns].fillna(df[numeric_columns].median())

				# Fill categorical nulls with 'Unknown'
				categorical_columns = df.select_dtypes(include=['object']).columns
				df[categorical_columns] = df[categorical_columns].fillna('Unknown')

				# Add processing timestamp
				df['processed_at'] = datetime.now()

				self.logger.info(f"Transformation completed. Final shape: {df.shape}")
				return df

		except Exception as e:
				self.logger.error(f"Error transforming data: {str(e)}")
				raise

	def load_data(self, df, filename):
			"""Load data to processed directory"""
			try:
					processed_path = os.path.join(self.config['paths']['processed_data'], f"{filename}.parquet")
					os.makedirs(os.path.dirname(processed_path), exist_ok=True)

					# Save as Parquet for better performance
					df.to_parquet(processed_path, index=False)

					# Also save as CSV for easy viewing
					csv_path = os.path.join(self.config['paths']['processed_data'], f"{filename}.csv")
					df.to_csv(csv_path, index=False)

					self.logger.info(f"Data loaded successfully to {processed_path}")

			except Exception as e:
					self.logger.error(f"Error loading data: {str(e)}")
					raise

	def run_pipeline(self, source_name="sample_sales"):
			"""Run the complete ETL pipeline"""
			try:
				self.logger.info(f"Starting pipeline for {source_name}")

				# Extract
				raw_df = self.extract_data(source_name)

				# Transform
				processed_df = self.transform_data(raw_df)

				# Load
				output_filename = f"{source_name}_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
				self.load_data(processed_df, output_filename)

				self.logger.info("Pipeline completed successfully!")
				return processed_df

			except Exception as e:
				self.logger.error(f"Pipeline failed: {str(e)}")
				raise

if __name__ == "__main__":
	pipeline = DataPipeline()
	result = pipeline.run_pipeline()
	print(f"Pipeline completed! Processed {len(result)} rows")
