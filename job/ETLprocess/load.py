import psycopg2
import pandas as pd

from sqlalchemy import create_engine

class Load(object):

	def __init__(self):
		self.dbname='rocket_launches'
		self.user='postgres'
		self.password='new_password'
		self.host='localhost'
		self.port='5432'

		self.conn = psycopg2.connect(
			dbname=self.dbname,
			user=self.user,
			password=self.password,
			host='localhost',
			port='5432'
		)

		self.cur = self.conn.cursor()

	def create_engine(self):
		return create_engine(f'postgresql://{self.user}:{self.password}@localhost:5432/{self.dbname}')

	def create_table(self):

		query = """
			CREATE TABLE IF NOT EXISTS rocket_launches_table (
			    id INTEGER,
			    image_url VARCHAR(255),
			    news_site VARCHAR(255),
			    published_at TIMESTAMP,
			    summary VARCHAR(500),
			    title VARCHAR(255),
			    updated_at TIMESTAMP,
			    url VARCHAR(255)
			);
			"""
		self.cur.execute(query)
		self.conn.commit()

	def load_data(self):
		self.create_table()
		df = pd.read_parquet("/home/lucas/Desktop/Python/ETLprocess/job/data_files/space_flight")
		engine = self.create_engine()
		df.to_sql('rocket_launches_table', engine, if_exists='append', index=False)
		self.conn.commit()
		self.close_sessions()

	def close_sessions(self):
		self.cur.close()
		self.conn.close()

if __name__ == "__main__":
	ld = Load()
	ld.load_data()