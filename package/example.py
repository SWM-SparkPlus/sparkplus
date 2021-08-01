from shapely.geometry import Point, Polygon
import sys
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType
import geopandas as gpd
import pandas as pd
import gis

spark, gdf = gis.gis_init()
# df = gis.coord_to_dong(spark, gdf, 127.043738, 37.503259)
# df.printSchema()
# df.show()

mySchema = StructType([
	StructField("X", DoubleType(), True),
	StructField("Y", DoubleType(), True)
])

myRow = Row(127.73311, 37.88673)
myRow1 = Row(127.73311, 37.88673)
myDf = spark.createDataFrame([myRow, myRow1], mySchema)
result = gis.coord_to_dong(spark, gdf, myDf, "X", "Y")
result.show()
