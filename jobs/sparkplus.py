import os
import sys

from pyspark.sql.functions import lit, udf, pandas_udf
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

import geopandas as gpd
import h3

class CustomDataFrame(DataFrame):

	def __init__(self, origin_df, x_colname, y_colname):
		self._origin_df = origin_df
		self.x_colname = x_colname
		self.y_colname = y_colname
		# self._gdf = gdf
	def create_sjoin_pnu(self, gdf, join_column_name):
		def sjoin_settlement(x, y):
			gdf_temp = gpd.GeoDataFrame(
				data=[[x] for x in range(len(x))], geometry=gpd.points_from_xy(x, y)
			).set_crs(epsg=4326, inplace=True)
			settlement = gpd.sjoin(gdf_temp, gdf, how='left', op='within')
			settlement = settlement.drop_duplicates(subset='geometry')

			return (
				settlement.agg({"PNU": lambda x: str(x)})
				.reset_index()
				.loc[:, join_column_name]
				.astype("str")
			)
		return pandas_udf(sjoin_settlement, returnType=StringType())
	
	def join_with_table(self, gdf, table_df):
		temp_df = self.coord_to_pnu(gdf)
		table_df = table_df.dropDuplicates(["bupjungdong_code"])
		res_df = temp_df.join(
			table_df, [temp_df.PNU[0:10] == table_df.bupjungdong_code], how='left_outer'
		)
		
		return res_df

	def coord_to_h3(self, h3_level):
		udf_to_h3 = udf(
			lambda x, y: h3.geo_to_h3(float(x), float(y), h3_level), returnType=StringType()
		)
		res_h3 = self._origin_df.withColumn("h3", udf_to_h3(self._origin_df[self.y_colname], self._origin_df[self.x_colname]))
		return res_h3

	
	def coord_to_pnu(self, gdf):
		sjoin_udf = self.create_sjoin_pnu(gdf, "PNU")
		res_df = self._origin_df.withColumn("PNU", sjoin_udf(self._origin_df[self.x_colname], self._origin_df[self.y_colname]))
		
		return res_df

	def coord_to_zip(self, gdf):
		self.origin_df = self

	def coord_to_emd(self, x_colname, y_colname):
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