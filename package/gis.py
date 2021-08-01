from shapely.geometry import Point, Polygon, LineString
from pyspark.sql import SparkSession
import geopandas as gpd

import numpy as np
import pandas as pd
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType,DecimalType
from pyspark.sql.functions import lit, pandas_udf, PandasUDFType

def gis_init():
	spark = SparkSession.builder.appName("SparkSession").getOrCreate()
	shp = "/root/spark-plugin/resource/EMD_202101/TL_SCCO_EMD.shp"
	korea = gpd.read_file(shp, encoding='euc-kr')
	gdf = korea.to_crs(4326)
	return spark, gdf

def coord_to_dong(spark, gdf, lng, lat):
	addr = gdf[gdf.geometry.contains(Point(lng, lat)) == True]
	addr_drop_geom = addr.drop(columns='geometry')
	df = spark.createDataFrame(addr_drop_geom)
	df = df.select(concat(df.EMD_CD, lit("00")).alias('EMD_CD'), 'EMD_ENG_NM', 'EMD_KOR_NM')
	return df

def spark_to_pandas(spark_df):
	return spark_df.select("*").toPands()

def pandas_to_geopandas(pandas_df):
	return gpd.GeoDataFrame(pandas_df)