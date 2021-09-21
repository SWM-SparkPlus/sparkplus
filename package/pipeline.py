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
import h3pandas
import boto3
import pyspark
from dotenv import load_dotenv

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

spark = SparkSession.builder.appName("Spark App").getOrCreate()
# dict = gis.load_table(s
# park) #table dictionary 불러오기
# jibun_dict = {}
# for key, val in list(dict.items()) :
# 	if 'jibun_address' in key :
# 		result = dict[key].select(['bupjungdong_code', 'sido', 'sigungu', 'bupjungeupmyeondong']).dropDuplicates(['bupjungdong_code']).orderBy("bupjungdong_code")
# 		jibun_dict[key] = result

gdf = gis.load_shp(spark, "../resource/EMD_202101/TL_SCCO_EMD.shp") #법정동 shp 파일 불러오기
gdf = gdf.h3.polyfill(10)
pd_h3 = pd.DataFrame(gdf)
del gdf
pd_h3 = pd_h3.drop('geometry', axis=1)
sdf = spark.createDataFrame(pd_h3)
sdf.coalesce(1).write.parquet("output/h3_10_pq_1")

"""df to parquet example
sdf_df = gis.gdf_to_spark_wkt(spark, gdf) #spark에서 읽을 수 있도록 wkt로 변환
gdf_h3 = gdf.h3.polyfill(11)
result_df = gis.gdf_to_spark_wkt(spark, gdf_h3)
sdf = spark.createDataFrame(pd.DataFrame(result_df))
sdf.write.parquet("output/gdf_h3_11.parquet")
"""