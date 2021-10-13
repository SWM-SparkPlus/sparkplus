import os
import sys

from pyspark.sql.functions import lit, udf, pandas_udf
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

import geopandas as gpd
import h3

def create_sjoin_pnu(gdf, join_column_name):
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

def _coord_to_pnu(origin_df, gdf, x_colname, y_colname):
	sjoin_udf = create_sjoin_pnu(gdf, "PNU")
	res_df = origin_df.withColumn("PNU", sjoin_udf(origin_df[x_colname], origin_df[y_colname]))
	return res_df

def _join_with_table(table_df, pnu_df):
    		# temp_df = self.coord_to_pnu()
	table_df = table_df.dropDuplicates(["bupjungdong_code"])
	res_df = pnu_df.join(
		table_df, [pnu_df.PNU[0:10] == table_df.bupjungdong_code], how='left_outer'
	)
	res_df = res_df.dropDuplicates(['PNU'])
		
	return res_df

class CoordDataFrame(DataFrame):
	"""
	Summary
	-------
	위경도 좌표가 포함된 Spark DataFrame에 법정읍면동, h3, 우편번호 정보를 추가합니다.

	Args:
		origin_sdf (Spark DataFrame):  위경도 좌표가 포함된 원본 Spark DataFrame
		gdf (GeoDataFrame): shp Parquet으로부터 생성한 GeoDataFrame
		tdf (Spark DataFrame): 데이터베이스로부터 생성한 Spark DataFrame
		x_colname (String): 원본 Spark DataFrame의 경도 컬럼 이름
		y_colname (String): 원본 Spark DataFrame의 위도 컬럼 이름

	Usage
	-------
	>>> from sparkplus.core.sparkplus import CoordDataFrame
	>>> df = CoordDataFrame(origin_sdf, gdf, tdf, x_colname, y_colname)
	"""

	def __init__(self, origin_sdf, gdf, tdf, x_colname, y_colname):
		self._origin_sdf = origin_sdf
		self._gdf = gdf
		self._tdf= tdf
		self._x_colname = x_colname
		self._y_colname = y_colname
		
		self.pnu_df = _coord_to_pnu(origin_sdf, gdf, x_colname, y_colname).cache()
		self.joined_df = _join_with_table(tdf, self.pnu_df).cache()

	def coord_to_h3(self, h3_level):
		"""
		Summary
		-------
		위경도 좌표가 포함된 원본 Spark DataFrame에 h3 정보를 추가합니다.

		Args:
			h3_level (Int): 추가하고자 하는 h3 level
		
		Usage
		-------
		>>> from sparkplus.core.sparkplus import CoordDataFrame
		>>> df = CoordDataFrame(origin_sdf, gdf, tdf, 'lon', 'lat')
		>>> res_df = df.coord_to_h3(10)
		"""
		udf_to_h3 = udf(
		lambda x, y: h3.geo_to_h3(float(x), float(y), h3_level), returnType=StringType()
		)

		res_h3 = self._origin_sdf.withColumn("h3", udf_to_h3(self._origin_sdf[self._y_colname], self._origin_sdf[self._x_colname]))
		return res_h3
	
	def coord_to_pnu(self):
		"""
		Summary
		-------
		위경도 좌표가 포함된 원본 Spark DataFrame에 pnu 정보를 추가합니다.
		
		Usage
		-------
		>>> from sparkplus.core.sparkplus import CoordDataFrame
		>>> df = CoordDataFrame(origin_sdf, gdf, tdf, 'lon', 'lat')
		>>> res_df = df.coord_to_pnu()
		"""
		return self.pnu_df

	def coord_to_zipcode(self):	
		"""
		Summary
		-------
		위경도 좌표가 포함된 원본 Spark DataFrame에 우편번호 정보를 추가합니다.
		
		Usage
		-------
		>>> from sparkplus.core.sparkplus import CoordDataFrame
		>>> df = CoordDataFrame(origin_sdf, gdf, tdf, 'lon', 'lat')
		>>> res_df = df.coord_to_zipcode()
		"""
		joined_df = self.joined_df.select("PNU", "zipcode")
		res_df = self.pnu_df.join(joined_df, "PNU", "leftouter")
		return res_df

	def coord_to_emd(self):
		"""
		Summary
		-------
		위경도 좌표가 포함된 원본 Spark DataFrame에 읍면동 코드 정보를 추가합니다.
		
		Usage
		-------
		>>> from sparkplus.core.sparkplus import CoordDataFrame
		>>> df = CoordDataFrame(origin_sdf, gdf, tdf, 'lon', 'lat')
		>>> res_df = df.coord_to_emd()
		"""
		joined_df= self.joined_df.select("PNU", "bupjungdong_code")
		res_df = self.pnu_df.join(joined_df, "PNU", "leftouter")
		return res_df

	def coord_to_roadname(self):
		"""
		Summary
		-------
		위경도 좌표가 포함된 원본 Spark DataFrame에 도로명 주소 정보를 추가합니다.
		
		Usage
		-------
		>>> from sparkplus.core.sparkplus import CoordDataFrame
		>>> df = CoordDataFrame(origin_sdf, gdf, tdf, 'lon', 'lat')
		>>> res_df = df.coord_to_roadname()
		"""
		joined_df= self.joined_df.select("PNU", "sido", "sigungu", "roadname", "eupmyeondong", "bupjunli", "is_basement", "building_primary_number", "building_secondary_number", "bupjungdong_code")
		res_df = self.pnu_df.join(joined_df, "PNU", "leftouter")
		return res_df

	def coord_to_jibun(self):
		"""
		Summary
		-------
		위경도 좌표가 포함된 원본 Spark DataFrame에 지번 주소 정보를 추가합니다.
		
		Usage
		-------
		>>> from sparkplus.core.sparkplus import CoordDataFrame
		>>> df = CoordDataFrame(origin_sdf, gdf, tdf, 'lon', 'lat')
		>>> res_df = df.coord_to_jibun()
		"""
		joined_df= self.joined_df.select("PNU", "sido", "sigungu", "eupmyeondong", "bupjungli", "jibun_primary_number", "jibun_secondary_number")
		res_df = self.pnu_df.join(joined_df, "PNU", "leftouter")
		return res_df

	def join_with_table(self):
		"""
		Summary
		-------
		위경도 좌표가 포함된 원본 Spark DataFrame에 데이터베이스에서 가져온 Spark DataFrame 정보를 추가합니다.
		
		Usage
		-------
		>>> from sparkplus.core.sparkplus import CoordDataFrame
		>>> df = CoordDataFrame(origin_sdf, gdf, tdf, 'lon', 'lat')
		>>> res_df = df.join_with_table()
		"""
		return self.joined_df

class RoadnameDataFrame(DataFrame):
	"""
	Summary
	-------
	도로명주소가 포함된 Spark DataFrame에 법정읍면동, 지번주소, h3, 우편번호 정보를 추가합니다.

	Args:
		origin_sdf (Spark DataFrame):  도로명주소가 포함된 원본 Spark DataFrame
		gdf (GeoDataFrame): shp Parquet으로부터 생성한 GeoDataFrame
		tdf (Spark DataFrame): 데이터베이스로부터 생성한 Spark DataFrame
		roadname_colname (String): 도로명주소 컬럼 이름

	Usage
	-------
	>>> from sparkplus.core.sparkplus import RoadnameDataFrame
	>>> df = RoadnameDataFrame(origin_sdf, gdf, tdf, roadname_colname)
	"""

	def __init__(self, origin_sdf, gdf, tdf, roadname_colname):
		pass
