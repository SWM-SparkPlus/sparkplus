from shapely.geometry import Point, Polygon
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import *
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType
import geopandas as gpd
import pandas as pd
import mysql.connector
import sys
import os
import gis

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

spark = SparkSession.builder.appName("Spark App").getOrCreate()

# table_list = [
# 	'additional_info_busan',
# 	'additional_info_chungbuk',
# 	'additional_info_chungnam',
# 	'additional_info_daegu',
# 	'additional_info_daejeon',
# 	'additional_info_gangwon',
# 	'additional_info_gwangju',
# 	'additional_info_gyeongbuk',
# 	'additional_info_gyeonggi',
# 	'additional_info_gyeongnam',
# 	'additional_info_incheon',
# 	'additional_info_jeju',
# 	'additional_info_jeonbuk',
# 	'additional_info_jeonnam',
# 	'additional_info_sejong',
# 	'additional_info_seoul',
# 	'additional_info_ulsan',
# 	'jibun_address_busan',
# 	'jibun_address_chungbuk',
# 	'jibun_address_chungnam',
# 	'jibun_address_daegu',
# 	'jibun_address_daejeon',
# 	'jibun_address_gangwon',
# 	'jibun_address_gwangju',
# 	'jibun_address_gyeongbuk',
# 	'jibun_address_gyeonggi',
# 	'jibun_address_gyeongnam',
# 	'jibun_address_incheon',
# 	'jibun_address_jeju',
# 	'jibun_address_jeonbuk',
# 	'jibun_address_jeonnam',
# 	'jibun_address_sejong',
# 	'jibun_address_seoul',
# 	'jibun_address_ulsan',
# 	'roadname_address_busan',
# 	'roadname_address_chungbuk',
# 	'roadname_address_chungnam',
# 	'roadname_address_daegu',
# 	'roadname_address_daejeon',
# 	'roadname_address_gangwon',
# 	'roadname_address_gwangju',
# 	'roadname_address_gyeongbuk',
# 	'roadname_address_gyeonggi',
# 	'roadname_address_gyeongnam',
# 	'roadname_address_incheon',
# 	'roadname_address_jeju',
# 	'roadname_address_jeonbuk',
# 	'roadname_address_jeonnam',
# 	'roadname_address_sejong',
# 	'roadname_address_seoul',
# 	'roadname_address_ulsan',
# 	'roadname_code'
# ]
# table_dict = {}

# for table in table_list:
#     name = table + "_df"
#     globals()[name] = gis.db_table_to_df(spark, table)

gdf = gis.gdf_init(spark, "../resource/TL_SCCO_EMD.shp")
sdf_df = gis.gdf_to_spark(spark, gdf)
ssdf = sdf_df.limit(10)
pdf = gis.spark_to_pandas(ssdf)
ggdf = gis.pandas_to_geopandas(pdf)
print(ggdf.head())