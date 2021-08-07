from shapely.geometry import Point, Polygon
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType
import geopandas as gpd
import pandas as pd
import mysql.connector

#sspark, gdf = gis.gis_init()
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
spark = SparkSession.builder.appName("hello").getOrCreate()

test_df = spark.read.format("jdbc").option("url", "jdbc:mysql://host.docker.internal:3306/sparkplus") \
    .option("driver", "com.mysql.cj.jdbc.Driver").option('dbtable', "jibun_address_daejeon") \
    .option("user", "root").option("password", "sparkplus").load()

test_df.show()