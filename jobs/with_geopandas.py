from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType,DecimalType
from pyspark.sql.functions import lit, pandas_udf, PandasUDFType

import pandas as pd
import geopandas as gpd

import sys 
import os 

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from dependencies.spark import start_spark

def geopandas_df_to_spark_for_points(spark, gdf):
    gdf['lon'] = gdf['geometry'].x
    gdf['lat'] = gdf['geometry'].y
    sdf = spark.createDataFrame(pd.DataFrame(gdf).drop(['geometry'], axis=1))
    return sdf



korea_shp_file = "../shp/TL_SCCO_LI.shp"

gdf = gpd.read_file(korea_shp_file, encoding='euc-kr')

print(gdf.columns)
print(gdf.geom_type)
print(gdf)

gdf = gdf.to_crs(4326)

print(gdf)
print(type(gdf))
print(gdf.columns)
print(gdf.geom_type)