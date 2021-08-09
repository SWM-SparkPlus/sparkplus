import sys
import os

import pandas as pd
import mysql.connector
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType
import geopandas as gpd

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from dependencies.spark import start_spark
from jobs.with_geopandas import geopandas_df_to_spark_for_points
from package import gis

def download_file(url):
    filename = url.split('/')[-1]
    response = urllib.request.urlopen(url)
    content = response.read()

spark, *_ = start_spark()

# test_spark = test_spark.read.csv('../train.csv', header=True, inferSchema=True).select(['SUBWAY_ID', 'STATN_ID', 'STATN_NM'])

# Spark with GeoPandas
# sdf = geopandas_df_to_spark_for_points(spark, gdf)
# display(sdf)
# Spark with MySQL


test_df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/sparkplus") \
    .option("driver", "com.mysql.cj.jdbc.Driver").option('dbtable', "jibun_address_seoul") \
    .option("user", "root").option("password", "9315").load()

test_df.show()
test_df = test_df.toPandas()
# print(test_df.iloc[1]['bupjungdong_code'])
#print(test_df.columns)
# print([test_df.bupjungdong_code=='1144012100'])

shp = "/Users/hwan/project/spark-plus/spark-plugin/resource/EMD_202101"
korea = gpd.read_file(shp, encoding='euc-kr')
gdf = korea.to_crs(4326)

# spark, gdf = gis.gis_init()
# df = gis.coord_to_dong(spark, gdf, 127.043738, 37.503259)
# df.printSchema()
# df.show()

hongdae_lat = 37.557435
hongdae_lng = 126.925808

mySchema = StructType([
	StructField("X", DoubleType(), True),
	StructField("Y", DoubleType(), True)
])

myRow = Row(126.92580, 37.55743)
myRow1 = Row(127.04483, 37.50380)
myDf = spark.createDataFrame([myRow, myRow1], mySchema)
result = gis.coord_to_dong(spark, gdf, myDf, "X", "Y")
result.show()

result_pd = result.toPandas()

EMD_CD1 = result_pd.iloc[0]['EMD_CD'] +'00'
EMD_CD2 = result_pd.iloc[1]['EMD_CD'] + '00'

test_df_1 = test_df[test_df.bupjungdong_code==EMD_CD1].iloc[0]
test_df_2 = test_df[test_df.bupjungdong_code==EMD_CD2].iloc[0]

print(test_df_1)
print(test_df_2)

sido_list = [test_df_1.sido_name, test_df_2.sido_name]
sigungu_list = [test_df_1.sigungu_name, test_df_2.sigungu_name]
bupjungdong_list = [test_df_1.bupjung_eupmyeondong_name , test_df_2.bupjung_eupmyeondong_name]



# print(test_df.iloc[0]['bupjungdong_code'==EMD_CD1])

import h3
import h3.api.basic_int as h3_int
import h3.api.numpy_int as h3_np
import h3.api.memview_int as h3_mv

from shapely.geometry import Point, Polygon
from geopy.distance import distance
import geopandas as gpd
import folium
import pandas as pd

hongdae_lat = 37.557435
hongdae_lng = 126.925808

anam_lat = 37.50380
anam_lng = 127.04483

"""
print('basic_str: ', h3.geo_to_h3(hongdae_lat, hongdae_lng, 7))
print('basic_int: ', h3_int.geo_to_h3(hongdae_lat, hongdae_lng, 7))
print('numpy_int: ', h3_np.geo_to_h3(hongdae_lat, hongdae_lng, 7))
print('memview_int: ', h3_mv.geo_to_h3(hongdae_lat, hongdae_lng, 7))
"""

anam_lv7 = h3.geo_to_h3(anam_lat, anam_lng, 7)
hongdae_lv7 = h3.geo_to_h3(hongdae_lat, hongdae_lng, 7)

print(anam_lv7)

def to_polygon(l):
    return Polygon(h3.h3_to_geo_boundary(l, geo_json=True))

df = gpd.GeoDataFrame({'h3': [anam_lv7, anam_lv7]})
df['geometry'] = df['h3'].apply(to_polygon)
df.crs = {'init': 'epsg:4326'}

print(df)

anam = [anam_lat, anam_lng]
m = folium.Map(anam, zoom_start=14)
folium.GeoJson(df).add_to(m)


m.save('anam_kr.html')

h3_list = [hongdae_lv7, anam_lv7]

result_pd.insert(len(result.columns), 'h3_lv7', h3_list)
result_pd.insert(len(result.columns), "bupjungdong", bupjungdong_list)
result_pd.insert(len(result.columns), "sigungu", sigungu_list)
result_pd.insert(len(result.columns), "sido", sido_list)


print(result_pd)