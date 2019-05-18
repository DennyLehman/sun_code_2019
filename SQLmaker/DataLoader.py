import psycopg2
from config import config
import os
import json
import csv

# https://wiki.postgresql.org/wiki/Using_psycopg2_with_PostgreSQL

# http://www.postgresqltutorial.com/postgresql-python/connect/


class DataLoader(object):
	"""docstring for DataLoader"""
	def __init__(self):
		self.app_folder = self.get_app_dir()

	def get_app_dir(self):
		
		return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

	def get_file_path(self, file_name, sub_directory=None):
		
		file_path = os.path.join(self.app_folder, sub_directory, file_name)

		return file_path

	def read_json_data_from_file(self, file_path):
		
		with open(file_path) as json_file:
			data = json.load(json_file)	
		return data
	
	''' TODO: make csv reader '''	
	def read_csv_data_from_file(self, file_path):
		
		with open(file_path, mode='r') as csv_file:
		
			reader = csv.reader(csv_file)
		
			data = list(reader)
		
		return data

	def get_json_data(self,file_name, sub_directory):
		
		file_path = self.get_file_path(file_name=file_name, sub_directory=sub_directory)
		
		data = self.read_json_data_from_file(file_path)
		
		return data

	def connect_to_postgres(self):
		
		print('connecting to postgresdb')
		
		params = self.get_postgres_params()
		
		self.conn = psycopg2.connect(**params)
		
		self.cur = self.conn.cursor()
		
		print('connection made')
		
		return True

	def get_postgres_params(self):

		print('Connection type: Cloud')

		return config(filename='database.ini')

	def disconnect(self):
		
		print('disconnecting from database')
		
		self.cur.close()
		
		self.conn.close()
		
		print('disconnected.')
		
		return True

	def execute_insert_query(self, sql_query):
		
		query_success = True
		
		try:
		
			#self.cur.execute(sql, queries)

			self.cur.execute(sql_query)
		
		except Exception as e:
			
			print('error on big query ')
			
			self.disconnect()
			
			query_success = False
	
			raise e

		self.conn.commit()

		return query_success

if __name__ == '__main__':
	dl = DataLoader()
	print(dl.app_folder)
	print(dl.cloud_connect)
	
	file = dl.get_file_path(file_name='site_225542_details.txt',sub_directory='site_details')

	print(dl.get_data(file))
	
	#data = dl.read_csv_data_from_file('csv_test.csv')
	#print(data)
