from shapely.geometry import Point, Polygon
<<<<<<< HEAD
import sys
import os
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
import findspark
findspark.init()
=======
>>>>>>> 6f69a90e7f54247696ec69eaf5faddc30b1245a6
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType
import geopandas as gpd
import pandas as pd
import mysql.connector

<<<<<<< HEAD
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from dependencies.spark import start_spark

spark, gdf = gis.gis_init()
=======
#sspark, gdf = gis.gis_init()
>>>>>>> 6f69a90e7f54247696ec69eaf5faddc30b1245a6
# df = gis.coord_to_dong(spark, gdf, 127.043738, 37.503259)
# df.printSchema()
# df.show()

# mySchema = StructType([
# 	StructField("X", DoubleType(), True),
# 	StructField("Y", DoubleType(), True)
# ])

# myRow = Row(127.73311, 37.88673)
# myRow1 = Row(127.73311, 37.88673)
# myDf = spark.createDataFrame([myRow, myRow1], mySchema)
# result = gis.coord_to_dong(spark, gdf, myDf, "X", "Y")
# result.show()
import sys

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

driver = "com.mysql.cj.jdbc.Driver"
url = "jdbc:mysql://host.docker.internal:3306/sparkplus"
user = "root"
password = "sparkplus"

<<<<<<< HEAD
myRow = Row(127.73311, 37.88673)
myRow1 = Row(127.83311, 37.98673)
myDf = spark.createDataFrame([myRow, myRow1], mySchema)
result = gis.coord_to_dong(spark, gdf, myDf, "X", "Y")

result.show()
=======
spark = SparkSession.builder.appName("hello").getOrCreate()
li = [
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
table_df_list = list()
for table_name in li:
    name = table_name + "_df"
    name = spark.read.format("jdbc")\
        .option("driver", "com.mysql.cj.jdbc.Driver")\
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", user)\
        .option("password", password)\
        .load()
    table_df_list.append(name)
table_df_list[2].show()
>>>>>>> 6f69a90e7f54247696ec69eaf5faddc30b1245a6
