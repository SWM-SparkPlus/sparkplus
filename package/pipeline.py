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
dict = gis.load_table(spark) #table dictionary 불러오기
jibun_dict = {}
for key, val in list(dict.items()) :
	if 'jibun_address' in key :
		result = dict[key].select(['bupjungdong_code', 'sido', 'sigungu', 'bupjungeupmyeondong']).dropDuplicates(['bupjungdong_code']).orderBy("bupjungdong_code")
		jibun_dict[key] = result
		del dict[key]
print(jibun_dict)
# gdf = gis.load_shp(spark, "../resource/TL_SCCO_EMD.shp") #법정동 shp 파일 불러오기
# sdf_df = gis.gdf_to_spark_wkt(spark, gdf) #spark에서 읽을 수 있도록 wkt로 변환
