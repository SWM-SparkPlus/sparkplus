from shapely.geometry import Point, Polygon
import sys
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
import findspark
findspark.init()
from pyspark.sql import SparkSession
import geopandas as gpd
import pandas as pd
import gis

spark, gdf = gis.gis_init()
df = gis.coord_to_dong(spark, gdf, 127.043738, 37.503259)
df.printSchema()
df.show()