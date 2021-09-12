import sys
import os

import pandas as pd
import geopandas as gpd 

from pyspark.sql import Row
from pyspark.sql.types import *
from shapely.geometry import Polygon
from pyspark.sql.functions import udf

import h3
import folium

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from dependencies.spark import start_spark
from jobs.table_to_df import create_df
from package import gis
from jobs.conversion import join_with_h3, join_with_emd, coord_to_h3, coord_to_jibun, coord_to_roadname

from pyspark.sql.types import StringType
from pyspark.sql.functions import col, pandas_udf

table_list = [
	'additional_info_busan',
	'additional_info_chungbuk',
	'additional_info_chungnam',
	'additional_info_daegu',
	'additional_info_daejeon',
	'additional_info_gangwon',
	'additional_info_gwangju',
	'additional_info_gyeongbuk',
	'additional_info_gyeonggi',
	'additional_info_gyeongnam',
	'additional_info_incheon',
	'additional_info_jeju',
	'additional_info_jeonbuk',
	'additional_info_jeonnam',
	'additional_info_sejong',
	'additional_info_seoul',
	'additional_info_ulsan',
	'jibun_address_busan',
	'jibun_address_chungbuk',
	'jibun_address_chungnam',
	'jibun_address_daegu',
	'jibun_address_daejeon',
	'jibun_address_gangwon',
	'jibun_address_gwangju',
	'jibun_address_gyeongbuk',
	'jibun_address_gyeonggi',
	'jibun_address_gyeongnam',
	'jibun_address_incheon',
	'jibun_address_jeju',
	'jibun_address_jeonbuk',
	'jibun_address_jeonnam',
	'jibun_address_sejong',
	'jibun_address_seoul',
	'jibun_address_ulsan',
	'roadname_address_busan',
	'roadname_address_chungbuk',
	'roadname_address_chungnam',
	'roadname_address_daegu',
	'roadname_address_daejeon',
	'roadname_address_gangwon',
	'roadname_address_gwangju',
	'roadname_address_gyeongbuk',
	'roadname_address_gyeonggi',
	'roadname_address_gyeongnam',
	'roadname_address_incheon',
	'roadname_address_jeju',
	'roadname_address_jeonbuk',
	'roadname_address_jeonnam',
	'roadname_address_sejong',
	'roadname_address_seoul',
	'roadname_address_ulsan',
	'roadname_code'
]

driver = "com.mysql.cj.jdbc.Driver"
url = "jdbc:mysql://localhost:3306/sparkplus"
user = "sparkplus"
password = "sparkplus"

# filepath = "../resource/LI_202101/TL_SCCO_LI.shp"
# table_name = "roadname_address_seoul"
filepath = "../resource/data/daegu_streetlight.csv"

spark, *_ = start_spark()

gdf = gis.load_shp(spark, "../resource/EMD_202101/TL_SCCO_EMD.shp")
gdf = gdf.to_crs(4326)

hongdae_lat = 37.55743
hongdae_lng = 126.92580

my_sdf = spark.read.option("header", True).format("csv").load(filepath, encoding="euc-kr")

# result = coord_to_emd(spark, gdf, sdf, "경도", "위도")
# result.show()
# result = coord_to_emd(spark, gdf, hongdae_lng, hongdae_lat)

# result.show()
"""
mySchema = StructType([
        StructField('x', DoubleType(), True),
        StructField('y', DoubleType(), True)
    ])
myRow = Row(hongdae_lng, hongdae_lat)
myDf = spark.createDataFrame([myRow], mySchema)

h3_df = coord_to_h3(hongdae_lng, hongdae_lat, 10)

jibun_df = coord_to_jibun(spark, gdf, jibun_address_seoul_df, hongdae_lng, hongdae_lat)

roadname_df = coord_to_roadname(spark, gdf, jibun_address_seoul_df, roadname_address_seoul_df, roadname_code_df, hongdae_lng, hongdae_lat)
"""
"""
def create_sjoin_udf(gdf_with_poly, join_column_name):
	def sjoin_settlement(x, y):
		gdf_temp = gpd.GeoDataFrame(data=[[x] for x in range(len(x))], geometry=gpd.points_from_xy(x, y), columns=['id'])
		# gdf_temp = gdf_temp.to_crs(4326)
		gdf_temp.set_crs(epsg=4326, inplace=True)
		settlement = gpd.sjoin(gdf_temp, gdf_with_poly, how='left', op='within')
		return settlement.agg({'EMD_CD': lambda x: str(x)}). \
	reset_index().loc[:, join_column_name].astype('str')

	return pandas_udf(sjoin_settlement, returnType=StringType())

sjoin_udf = create_sjoin_udf(gdf, "EMD_CD")

res_df = my_sdf.withColumn("EMD_CD", sjoin_udf(my_sdf.경도, my_sdf.위도))
res_df.show()
"""

# res_emd = join_with_emd(gdf, my_sdf, '경도', '위도')
# res_emd.show()

"""
def to_polygon(l):
	return Polygon(h3.h3_to_geo_boundary(l, geo_json=True))

gdf_h3 = res_h3.toPandas()
gdf_h3 = gpd.GeoDataFrame(gdf_h3)
gdf_h3['geometry'] = gdf_h3['h3'].apply(to_polygon)
gdf_h3.crs = {'init': 'epsg:4326'}
print(gdf_h3)

temp = [35.8734, 128.6103]
m =folium.Map(temp, zoom_start=14)
folium.GeoJson(gdf_h3).add_to(m)

m.save('daegu.html')

"""

res = join_with_h3(my_sdf, '경도', '위도', 10)
res.show()