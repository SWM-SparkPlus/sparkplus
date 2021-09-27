from shapely.geometry import Point, Polygon
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import *
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType
import geopandas as gpd
import pandas as pd
import mysql.connector
import sys
from . import gis
import pyspark
from dotenv import load_dotenv

sys.stdout = open(sys.stdout.fileno(), mode="w", encoding="utf8", buffering=1)

spark = SparkSession.builder.appName("Spark App").getOrCreate()
dict = gis.load_table(spark)  # table dictionary 불러오기
jibun_dict = {}
for key, val in list(dict.items()):
    if "jibun_address" in key:
        result = (
            dict[key]
            .select(["bupjungdong_code", "sido", "sigungu", "bupjungeupmyeondong"])
            .dropDuplicates(["bupjungdong_code"])
            .orderBy("bupjungdong_code")
        )
        jibun_dict[key] = result

""" shp to polyfill
gdf = gis.load_shp(spark, "../resource/EMD_202101/TL_SCCO_EMD.shp") #법정동 shp 파일 불러오기
gdf = gdf.h3.polyfill(10)
pd_h3 = pd.DataFrame(gdf)
del gdf
pd_h3 = pd_h3.drop('geometry', axis=1)
sdf = spark.createDataFrame(pd_h3)
"""

""" sdf to json
sdf.coalesce(1).write.json('v1') #v1이라는 폴더가 생성됨
sdf.write.json('v2')
"""

"""
sdf_df = gis.gdf_to_spark_wkt(spark, gdf) #spark에서 읽을 수 있도록 wkt로 변환
result_df = gis.gdf_to_spark_wkt(spark, gdf_h3)
"""

""" read parquet
df = spark.read.option("mergeSchema", "true").parquet("../resource/h3/part-00000-3c1357f3-ca16-420a-8b7f-7e532d32c650-c000.snappy.parquet")
df.printSchema()
df.show()
"""
