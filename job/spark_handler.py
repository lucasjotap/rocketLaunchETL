from pyspark.sql import SparkSession

class SparkHandler:
	"""
	Class SparkHandler holds only a method for instantiating a SparkSession.
	"""
	spark = None

	@classmethod
	def create_session(cls):
		if cls.spark is None:
			cls.spark = (
				SparkSession
				.builder
				.appName("getCash")
				.getOrCreate()
			)
		return cls.spark