from DataLoader import DataLoader
import pandas as pd

class AllDataLoader(DataLoader):
	"""docstring for AllDataLoader"""
	def __init__(self):
		super(AllDataLoader, self).__init__()
		self.file_name = 'Powerhouse_SunCode_SVCE_Data.csv'

	def get_data(self):
		
		file_path = self.get_file_path(file_name='Powerhouse_SunCode_SVCE_Data.csv',sub_directory='raw_data')

		d = self.read_csv_data_from_file(file_path=file_path)
	
		print(d[1])
		return d[1:]

	def pivot_month_data(self):
		df = pd.read_csv(self.get_file_path(file_name='Powerhouse_SunCode_SVCE_Data.csv',sub_directory='raw_data'))
		
		pass


	def make_house_sql_string(self, data):
		sql = """
		INSERT INTO house_data (city, end_use, solar, bedrooms, garage, heat_air_cond, total_area, total_rooms, built_year, advanced_vehicle) VALUES
		"""
		for d in data:
			
			sql += '(\'{0}\',\'{1}\',\'{2}\',\'{3}\',\'{4}\',\'{5}\',{6},\'{7}\',\'{8}\',\'{9}\'), '.format(d[0],d[1],d[26],d[27],d[28],d[29],d[30],d[31],d[32],d[57])
		
		sql = sql[:-2]

		return sql

	def load_table_house_data(self):
		# get data
		data = self.get_data()
		# make sql string
		sql = self.make_house_sql_string(data)
		#run query		
		self.run_query(sql)

		return True

	def run_query(self,sql):
		
		if self.connect_to_postgres():
			
			self.execute_insert_query(sql)

		self.disconnect()

		pass


if __name__ == '__main__':
	adl = AllDataLoader()
	adl.load_table_house_data()

	# check with marta on map overlay
	# who - introduce population, show dash
		# low income\
		# high gas usage
	# where are they - show map
	# what do they need - show need
	# whats the solution - policy, money, whatever


