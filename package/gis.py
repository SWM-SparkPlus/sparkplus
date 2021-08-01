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

def coord_to_dong(spark, gdf, spark_df, lng_colname, lat_colname):
	p_df = spark_to_pandas(spark_df)
	g_df = gpd.GeoDataFrame(p_df, geometry = gpd.points_from_xy(p_df[lng_colname], p_df[lat_colname]))
	li = list()
	for i in g_df.index:
		for j in gdf.index:
			if gdf.geometry[j].contains(g_df.geometry[i]):
				li.append(gdf.EMD_CD[j])
	g_df.insert(len(g_df.columns), "EMD_CD", li)
	g_df = g_df.drop(columns="geometry")
	return spark.createDataFrame(g_df)

def spark_to_pandas(spark_df):
	return spark_df.select("*").toPandas()

def pandas_to_geopandas(pandas_df):
	return gpd.GeoDataFrame(pandas_df)