from DataLoader import DataLoader
import pandas as pd
import random

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

	def pivot_gas(self):
		file_name = self.get_file_path(file_name=self.file_name,sub_directory='raw_data')
		months = ['thrm_jan','thrm_feb','thrm_mar','thrm_apr','thrm_may','thrm_jun','thrm_jul','thrm_aug','thrm_sep','thrm_oct','thrm_nov','thrm_dec']
		l = self.pivot_month_data(file_name, months)
		
		return l

	def pivot_electric(self):
		file_name = self.get_file_path(file_name='Powerhouse_SunCode_SVCE_Data.csv',sub_directory='raw_data')
		months = ['KWH_JAN_2018','KWH_FEB_2018','KWH_MAR_2018','KWH_APR_2018','KWH_MAY_2018','KWH_JUN_2018','KWH_JUL_2018','KWH_AUG_2018','KWH_SEP_2018','KWH_OCT_2018','KWH_NOV_2018','KWH_DEC_2018']
		#l = self.pivot_month_data(file_name, months)
		
		pass
	

	def pivot_month_data(self,file_name,month_columns):
		#df = pd.read_csv(self.get_file_path(file_name='Powerhouse_SunCode_SVCE_Data.csv',sub_directory='raw_data'))
		df = pd.read_csv(file_name)
		#months = ['thrm_jan','thrm_feb','thrm_mar','thrm_apr','thrm_may','thrm_jun','thrm_jul','thrm_aug','thrm_sep','thrm_oct','thrm_nov','thrm_dec']
		i = 1
		#new_df = pd.DataFrame(columns = ['1','2','3','4','5','6','7','8','9','10','11','12'])
		new_df = pd.DataFrame(columns=['A'])
		for m in month_columns:
			print(i,m)
			new_df['{}'.format(i)] = df[m]
			i = i+1

		new_df = new_df.reset_index()
		new_df['house_id'] = new_df['index']
		new_df = new_df.drop(columns=['A','index'])
		num_months = ['1','2','3','4','5','6','7','8','9','10','11','12']
		new_df = pd.melt(new_df,id_vars=['house_id'])
		new_df = new_df.fillna(0)

		print(new_df.head())


		return new_df.values.tolist()


	def make_gas_sql_string(self,data):
		sql = """
		INSERT INTO thermal_data (house_id, month, thermal) VALUES 
		"""
		for d in data:
			#print(d)
			sql += '({},{},{}), '.format(d[0],d[1],d[2])

		return sql[:-2]

	def get_zip_code(self,city):
		
		sunnyvale = ['94085','94086','94088','94089']
		saratoga = ['95070','95071']
		los_altos = ['94022','94023','94024']
		campbell = ['95008','95009','95011']
		gilroy = ['95020', '95021']
		santa_clara_unincorp = ['95050','95051','95052','95053','95054','95055','95056']
		cupertino = ['95014','95015']
		mountain_view = ['94035','94036','94037','94038','94039','94040','94041','94042','94043']
		milpitas = ['95035','95036']
		morgan_hill = ['95037','95038']
		monte_sereno = ['95030']
		los_gatos = ['95030','95031','95032','95033']

		d ={
			'CAMPBELL':campbell,'CUPERTINO':cupertino ,'GILROY':gilroy,'LOS ALTOS':los_altos,'LOS ALTOS HILLS':los_altos,'LOS GATOS':los_gatos,'MILPITAS':milpitas,'MONTE SERENO':monte_sereno,
			'MORGAN HILL':morgan_hill,'MOUNTAIN VIEW':mountain_view,'SARATOGA':saratoga,'SUNNYVALE':sunnyvale,'UNINCORPORATED': santa_clara_unincorp
			}

		ran = random.randint(0,len(d[city])-1)
		#print(city, ran)
		_zip = d[city][ran]


		return _zip

	def make_house_sql_string(self, data):
		sql = """
		INSERT INTO house_data (city, end_use, solar, bedrooms, garage, heat_air_cond, total_area, total_rooms, built_year, advanced_vehicle, state, zip_code) VALUES
		"""


		for d in data:
			_zip = self.get_zip_code(d[0])
			sql += '(\'{0}\',\'{1}\',\'{2}\',\'{3}\',\'{4}\',\'{5}\',{6},\'{7}\',\'{8}\',\'{9}\',\'CA\',\'{10}\'), '.format(d[0],d[1],d[26],d[27],d[28],d[29],d[30],d[31],d[32],d[57],_zip)
		
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

	def run_gas_data(self):
		data = self.pivot_gas()
		sql = self.make_gas_sql_string(data)
		success = self.run_query(sql)
		return success


if __name__ == '__main__':
	adl = AllDataLoader()
	adl.load_table_house_data()
	#print(adl.pivot_month_data()[0:5])
	#adl.run_gas_data()
	#adl.pivot_gas()
	#print(adl.get_zip_code('CAMPBELL'))

	# check with marta on map overlay
	# who - introduce population, show dash
		# low income\
		# high gas usage
	# where are they - show map
	# what do they need - show need
	# whats the solution - policy, money, whatever


