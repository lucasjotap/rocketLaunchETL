import json
import logging
import requests

from typing import Dict, List 
from requests import Response
from pyspark.sql import SparkSession


class SparkHandler:
	self.spark = None
	"""
	Class SparkHandler holds only a method for instantiating a SparkSession.
	"""
	@classmethod
	def create_session(self):
		return spark = SparkSession.builder.getOrCreate() if spark == None else self.spark

class ExtractJob(Job):
	"""
    ExtractJob handles the API extraction from the Space Flight News API.	

    Methods:
		job_name -> string
		get_data_from_api -> dict
		get_all_pages -> list
		api_to_parquet -> None
	"""

	@property
	def job_name(self) -> str:
		return "extract"
	
	def get_data_from_api(self, page: int) -> Dict:
		"""
		Extracts data from API by page.
		
		Arguments:
			page (int): The page number to retrieve.

		Returns:
			Dict: The JSON data from the API response.
					Returns None if the request is unsuccessful.

		"""
		URL: str = f"https://api.spaceflightnewsapi.net/v4/reports/?offset={page}"
		response: Response = requests.get(URL)
		print(URL)
		return response.json() if response.status_code == 200 else None

	def get_all_pages(self) -> list:
		"""
		Retrieves every page from the API.
			
		Returns:
			List: A list containing data from all API pages.		
		"""
		all_pages: List = []
		page: int = 0

		while True:
			response: Response = self.get_data_from_api(page)
			if (response):
				result = response.get("results")
				all_pages.extend(result)

			next_page: dict = response.get("next")

			if (not next_page):
				break

			page += 10

		return all_pages

	def api_data_to_parquet(self) -> None:
		"""
        Writes API data into a Parquet file at 'ETLprocess/lake/space_flight/'.

        This method converts API data to a JSON string, creates a Spark DataFrame,
        and writes the DataFrame to a Parquet file.

        Returns:
            None
        """
		data = json.dumps(self.get_all_pages())
		spark = SparkHandler.create_session()
		df = spark.createDataFrame(data=self.get_all_pages())
		df = df.repartition(1)
		df.printSchema()
		df.write.parquet("ETLprocess/lake/space_flight/")

	def run(self):
		self.api_data_to_parquet()
