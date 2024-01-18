from extract import SparkHandler
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit

class TransformationJob:
	"""Class's purpose is to do transformation on DataFrames."""
	spark: SparkSession = SparkHandler.create_session()

	def read_data(self) -> DataFrame:
		"""Returns a DataFrame object"""
		df: DataFrame  = self.spark\
			.read\
			.option("inferSchema", "true")\
			.option("header", "true")\
			.format("parquet")\
			.load("data_files/space_flight/part-00000-ad7602fa-ad5a-4185-be00-bc2db798fcaf-c000.snappy.parquet")
		return df

	def transform_data(self):
		"""Method does a simple transformation on a DataFrame."""
		df: DataFrame = self.read_data()
		df = df.drop("summary").sort(df.updated_at.desc())
		df.write.parquet("data_files/transformation_one")


transform_data = TransformationJob()
transform_data.transform_data()
