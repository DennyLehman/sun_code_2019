from DataLoader import DataLoader

class AllDataLoader(DataLoader):
	"""docstring for AllDataLoader"""
	def __init__(self):
		super(AllDataLoader, self).__init__()
		self.file_name = 'Powerhouse_SunCode_SVCE_Data.csv'

	def main(self):
		file_path = self.get_file_path(file_name='Powerhouse_SunCode_SVCE_Data.csv')

		d = self.read_csv_data_from_file(file_name=file_path)
		return d
		



if __name__ == '__main__':
	adl = AllDataLoader()
	adl.main()

	# check with marta on map overlay
	# who - introduce population, show dash
		# low income\
		# high gas usage
	# where are they - show map
	# what do they need - show need
	# whats the solution - policy, money, whatever


