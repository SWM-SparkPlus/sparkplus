from jobs.conversion import join_with_table
from pyspark.sql.functions import lit, udf
from pyspark.sql import Dataframe
import h3

class CustomDataFrame(Dataframe):

	def __init__(self, origin_df):
		self.origin_df = origin_df

	def coord_to_h3(self, x_colname, y_colname, h3_level):
		udf_to_h3 = udf(
        	lambda x, y: h3.geo_to_h3(float(x), float(y), h3_level), returnType=StringType()
    	)
		res_h3 = self.origin_df.withColumn("h3", udf_to_h3(self.origin_df[y_colname], self.origin_df[x_colname]))
		return res_h3

	def coord_to_zip(self):
		self.origin_df = self

	def coord_to_emd(self):
		self.origin_df = self

	def coord_to_address(self):
		self.origin_df = self
	
	def address_to_h3(self):
		self.origin_df = self

	def address_to_zip(self):
		self.origin_df = self

	def address_to_emd(self):
		self.origin_df = self

	def address_to_coord(self):
		self.origin_df = self