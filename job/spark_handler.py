from pyspark.sql import SparkSession

class SparkHandler:
	self.spark = None
	"""
	Class SparkHandler holds only a method for instantiating a SparkSession.
	"""
	@classmethod
	def create_session(self):
		return spark = SparkSession.builder.getOrCreate() if spark == None else self.spark