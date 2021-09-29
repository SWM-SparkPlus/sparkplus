from pyspark.sql.functions import lit, udf
from pyspark.sql.types import StringType
import h3
from pyspark.sql import DataFrame

class CustomDataFrame(DataFrame):

	def __init__(self, origin_df: DataFrame):
		self.origin_df = origin_df
		self.gpd = None

	def set_gpd(self, gpd):
		self.gpd = gpd

	def coord_to_h3(self, x_colname, y_colname, h3_level, append=False):
		"""
		This function append or replaces h3 columns to the original data frame
		based on the x,y coordinates.
		If append is False, drop x,y column
		"""
		append_h3 = udf(
        	lambda x, y: h3.geo_to_h3(float(x), float(y), h3_level), returnType=StringType()
    	)
		res_h3 = self.origin_df.withColumn("h3", append_h3(self.origin_df[y_colname], self.origin_df[x_colname]))
		if not append:
			res_h3 = res_h3.drop(x_colname)
			res_h3 = res_h3.drop(y_colname)
		return res_h3

	def coord_to_zip(self, x_colname, y_colname, append=False):
		self.origin_df = self

	def coord_to_emd(self, x_colname, y_colname, append=False):
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

	def coord_to_point(lng_colname, lat_colname):
		df