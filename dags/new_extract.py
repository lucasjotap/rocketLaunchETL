import json
import requests
from typing import List
from requests import Response
from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor

class SparkHandler:

    @classmethod
    def create_session(cls):
        return SparkSession.builder.getOrCreate()

class ExtractJob:
    def __init__(self):
        self.session = requests.Session()

    @property
    def job_name(self) -> str:
        return "extract"

    def get_data_from_api(self, page) -> List:
        """Extracts data from API by page."""
        URL: str = f"https://api.spaceflightnewsapi.net/v4/reports/?offset={page}"
        with self.session.get(URL) as request:
            return request.json() if request.status_code == 200 else []

    def get_all_pages(self) -> List:
        """Retrieves every page from the API."""
        all_pages: List = []
        page_num: int = 0

        with ThreadPoolExecutor() as executor:
            while True:
                response = executor.submit(self.get_data_from_api, page_num).result()
                if response:
                    result = response.get("results", [])
                    all_pages.extend(result)

                next_page = response.get("next")

                if not next_page:
                    break

                page_num += 10

        return all_pages

    def api_data_to_parquet(self) -> None:
        """Returns data from API into a parquet file @ data_files/."""
        data = json.dumps(self.get_all_pages())
        spark = SparkHandler.create_session()
        df = spark.read.json(spark.sparkContext.parallelize([data]))
        df.write.parquet("data_files/space_flight")

    @classmethod
    def run(cls):
        job = cls()
        job.api_data_to_parquet()

if __name__ == "__main__":
    ExtractJob.run()
