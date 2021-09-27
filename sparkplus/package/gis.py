from shapely.geometry import Point, Polygon, LineString
from pyspark.sql import SparkSession
import geopandas as gpd

import numpy as np
import pandas as pd
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import (
    IntegerType,
    StringType,
    FloatType,
    DecimalType,
    DoubleType,
)
import os
from pyspark.sql.functions import lit, pandas_udf, PandasUDFType
from dotenv import load_dotenv

load_dotenv()


def load_shp(spark, file_location):
    korea = gpd.read_file(file_location, encoding="euc-kr")
    gdf = korea.to_crs(4326)
    return gdf


# def coord_to_dong(spark, gdf, lng, lat):
#     addr = gdf[gdf.geometry.contains(Point(lng, lat)) == True]
#     addr_drop_geom = addr.drop(columns="geometry")
#     df = spark.createDataFrame(addr_drop_geom)
#     df = df.select(
#         concat(df.EMD_CD, lit("00")).alias("EMD_CD"), "EMD_ENG_NM", "EMD_KOR_NM"
#     )
#     return df


def coord_to_dong(spark, gdf, spark_df, lng_colname, lat_colname):

    p_df = spark_to_pandas(spark_df)
    # geometry = gpd.points_from_xy(p_df['longitude'], p_df['latitude'])
    print("p_df: ", p_df)
    g_df = gpd.GeoDataFrame(
        p_df, geometry=gpd.points_from_xy(p_df[lng_colname], p_df[lat_colname])
    )
    # g_df = gpd.GeoDataFrame(p_df, geometry=geometry)
    print("g_df: ", g_df)
    li = list()
    for i in g_df.index:
        for j in gdf.index:
            if gdf.geometry[j].contains(g_df.geometry[i]):
                li.append(gdf.EMD_CD[j])
            # if j == 1: print(gdf.geometry[j], p_df.geometry[i])

    g_df.insert(len(g_df.columns), "EMD_CD", li)
    # g_df = g_df.drop(columns="geometry")
    g_df = spark.createDataFrame(g_df)

    return g_df


def spark_to_pandas(spark_df):
    return spark_df.select("*").toPandas()


def pandas_to_geopandas(pandas_df):
    return gpd.GeoDataFrame(pandas_df)


def db_table_to_df(spark, table):
    df = (
        spark.read.format("jdbc")
        .option("driver", os.getenv("DB_DRIVER"))
        .option("url", os.getenv("DB_URL"))
        .option("dbtable", table)
        .option("user", os.getenv("DB_USER"))
        .option("password", os.getenv("DB_PASSWORD"))
        .load()
    )
    return df


def gdf_to_spark_wkt(spark, gdf):
    gdf["wkt"] = pd.Series(
        map(lambda geom: str(geom.to_wkt()), gdf["geometry"]),
        index=gdf.index,
        dtype="str",
    )
    tmp = gdf.drop("geometry", axis=1)
    df = pd.DataFrame(tmp)
    sdf = spark.createDataFrame(tmp).cache()
    del tmp

    return sdf, df


def spark_to_gdf_wkt(spark, gdf, col_name):
    gdf["wkt_to_geom"] = gpd.GeoSeries.from_wkt(gdf[col_name])
    return gdf


def load_table(spark):
    table_list = [
        "additional_info_busan",
        "additional_info_chungbuk",
        "additional_info_chungnam",
        "additional_info_daegu",
        "additional_info_daejeon",
        "additional_info_gangwon",
        "additional_info_gwangju",
        "additional_info_gyeongbuk",
        "additional_info_gyeonggi",
        "additional_info_gyeongnam",
        "additional_info_incheon",
        "additional_info_jeju",
        "additional_info_jeonbuk",
        "additional_info_jeonnam",
        "additional_info_sejong",
        "additional_info_seoul",
        "additional_info_ulsan",
        "jibun_address_busan",
        "jibun_address_chungbuk",
        "jibun_address_chungnam",
        "jibun_address_daegu",
        "jibun_address_daejeon",
        "jibun_address_gangwon",
        "jibun_address_gwangju",
        "jibun_address_gyeongbuk",
        "jibun_address_gyeonggi",
        "jibun_address_gyeongnam",
        "jibun_address_incheon",
        "jibun_address_jeju",
        "jibun_address_jeonbuk",
        "jibun_address_jeonnam",
        "jibun_address_sejong",
        "jibun_address_seoul",
        "jibun_address_ulsan",
        "roadname_address_busan",
        "roadname_address_chungbuk",
        "roadname_address_chungnam",
        "roadname_address_daegu",
        "roadname_address_daejeon",
        "roadname_address_gangwon",
        "roadname_address_gwangju",
        "roadname_address_gyeongbuk",
        "roadname_address_gyeonggi",
        "roadname_address_gyeongnam",
        "roadname_address_incheon",
        "roadname_address_jeju",
        "roadname_address_jeonbuk",
        "roadname_address_jeonnam",
        "roadname_address_sejong",
        "roadname_address_seoul",
        "roadname_address_ulsan",
        "roadname_code",
    ]

    for table in table_list:
        name = table + "_df"
        globals()[name] = db_table_to_df(spark, table)
    return globals()
