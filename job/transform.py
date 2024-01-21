import os
from schema import custom_schema
from spark_handler import SparkHandler
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit

class TransformationJob:
	"""Class's purpose is to do transformation on DataFrames."""
	spark: SparkSession = SparkHandler.create_session()

	def read_data(self) -> DataFrame:
		"""Returns a DataFrame object"""
		df: DataFrame  = self.spark\
			.read\
			.schema(custom_schema)\
			.option("header", "true")\
			.format("parquet")\
			.load("/home/lucas/Desktop/Python/ETLprocess/job/data_files/space_flight/.part-00000-307df369-78fe-4d78-9f1a-0ea50efc2856-c000.snappy.parquet.crc")
		return df

	def transform_data(self):
		"""Method does a simple transformation on a DataFrame."""
		df: DataFrame = self.read_data()
		df = df.drop("summary").sort(df.updated_at.desc())
		df.write.parquet("ETLprocess/lake/analytics/space_flight/data/")

transform_data = TransformationJob()
transform_data.transform_data()
