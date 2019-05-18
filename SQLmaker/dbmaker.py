import psycopg2
#from config import config
from DataLoader import DataLoader

class MakeDB(DataLoader):
	"""docstring for MakeDB"""
	def __init__(self):
		super(MakeDB, self).__init__()

	def print_hi(self):
		print('hi')

	def make_table(self):
		commands = (
		""" 
		DROP TABLE retail_energy_usage;
		""",
		"""
		CREATE TABLE retail_energy_usage
		(
			row_id serial,
			house_id varchar,
			month int,
			kwh int
			
		)
		"""
		)
		pass

if __name__ == '__main__':
	
	db = MakeDB()
	db.print_hi()