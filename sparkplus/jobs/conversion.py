from geopandas.array import points_from_xy
from geopandas.tools.sjoin import sjoin
from shapely.geometry import Point, Polygon
from pyspark.sql import Row
from pyspark.sql.functions import lit, udf
from pyspark.sql.types import *
from pyspark.sql.functions import col, pandas_udf

import geopandas as gpd
import pandas as pd
import h3

import os

def shp_init():
    shp = gpd.read_file(os.path.dirname(os.path.abspath(__file__)) + "/../resource/EMD_202101/TL_SCCO_EMD.shp")
    shp = shp.to_crs(4326)
    return shp


def coord_to_dong(spark, gdf, lng, lat):
    addr = gdf[gdf.geometry.contains(Point(lng, lat)) == True]
    addr_drop_geom = addr.drop(columns="geometry")
    sdf = spark.createDataFrame(addr_drop_geom)
    sdf = sdf.select(
        concat(sdf.EMD_CD, lit("00")).alias("EMD_CD"), "EMD_ENG_NM", "EMD_KOR_NM"
    )
    return sdf


def coord_to_point(spark, df, lng_colname, lat_colname):
    df["temp"] = [Point(lon, lat) for lon, lat in df[[lng_colname, lat_colname]].values]
    df["point"] = pd.Series(
        map(lambda geom: str(geom.to_wkt()), df["temp"]), index=df.index, dtype="str"
    )
    tmp = df.drop("temp", axis=1)
    res_df = pd.DataFrame(tmp)
    res_sdf = spark.createDataFrame(tmp).cache()
    del tmp
    return res_sdf, res_df


def coord_file_to_emd(spark, gdf, filepath, lng_colname, lat_colname):
    _gdf = (
        spark.read.option("header", True)
        .format("csv")
        .load(filepath, encoding="euc-kr")
    )
    # _gdf = spark.createDataFrame(_file)
    _gdf.show()
    pdf = _gdf.select("*").toPandas()
    g_df = gpd.GeoDataFrame(
        pdf, geometry=gpd.points_from_xy(pdf[lng_colname], pdf[lat_colname])
    )
    li = list()
    for i in g_df.index:
        for j in gdf.index:
            if gdf.geometry[j].contains(g_df.geometry[i]):
                li.append(gdf.EMD_CD[j])
    g_df.insert(len(g_df.columns), "EMD_CD", li)
    g_df = spark.createDataFrame(g_df)
    return g_df


def coord_to_emd(spark, gdf, sdf, lng_colname, lat_colname):

    pdf = sdf.select("*").toPandas()
    # pdf = sdf
    g_df = gpd.GeoDataFrame(
        pdf, geometry=gpd.points_from_xy(pdf[lng_colname], pdf[lat_colname])
    )
    li = list()
    for i in g_df.index:
        for j in gdf.index:
            if gdf.geometry[j].contains(g_df.geometry[i]):
                print(g_df.geometry[i], gdf.EMD_CD[j])
                li.append(gdf.EMD_CD[j])
    g_df.insert(len(g_df.columns), "EMD_CD", li)
    g_df = spark.createDataFrame(g_df)
    return g_df


def coord_to_emd(spark, gdf, lng, lat, lng_colname="lng", lat_colname="lat"):
    mySchema = StructType(
        [
            StructField(lng_colname, DoubleType(), True),
            StructField(lat_colname, DoubleType(), True),
        ]
    )
    myRow = Row(lng, lat)
    myDf = spark.createDataFrame([myRow], mySchema)
    result = coord_df_to_emd(spark, gdf, myDf, lng_colname, lat_colname)
    return result


def to_polygon(l):
    return Polygon(h3.h3_to_geo_boundary(l, geo_json=True))


def coord_to_h3(lng, lat, h3_level):
    my_h3 = h3.geo_to_h3(lat, lng, h3_level)
    h3_df = gpd.GeoDataFrame({"h3": [my_h3, my_h3]})
    h3_df["geometry"] = h3_df["h3"].apply(to_polygon)
    h3_df.crs = {"init": "epsg:4326"}
    return h3_df


def coord_to_jibun(spark, gdf, table_df, lng, lat):
    emd_df = coord_to_emd(spark, gdf, lng, lat).toPandas()
    emd_cd = emd_df.iloc[0]["EMD_CD"] + "00"
    jibun_df = table_df[table_df["bupjungdong_code"] == emd_cd].toPandas()
    print(jibun_df)
    return jibun_df


def coord_to_roadname(
    spark, gdf, table_jibun, table_roadname, table_roadname_code, lng, lat
):
    jibun_df = coord_to_jibun(spark, gdf, table_jibun, lng, lat)
    manage_number = jibun_df.iloc[0]["manage_number"]
    roadname_code_df = table_roadname[
        table_roadname["manage_number"] == manage_number
    ].toPandas()
    roadname_code = roadname_code_df.iloc[0]["roadname_code"]
    result = table_roadname_code[table_roadname_code["roadname_code"] == roadname_code]
    return result


def create_sjoin_emd(gdf_poly, join_column_name):
    def sjoin_settlement(x, y):
        gdf_temp = gpd.GeoDataFrame(
            data=[[x] for x in range(len(x))], geometry=gpd.points_from_xy(x, y)
        ).set_crs(epsg=4326, inplace=True)
        settlement = gpd.sjoin(gdf_temp, gdf_poly, how="left", op="within")
        settlement = settlement.drop_duplicates(subset="geometry")
        # print(settlement.agg({'EMD_CD': lambda x: str(x) + '00'}).reset_index().loc[:, join_column_name].astype('str'))
        return (
            settlement.agg({"EMD_CD": lambda x: str(x) + "00"})
            .reset_index()
            .loc[:, join_column_name]
            .astype("str")
        )

    return pandas_udf(sjoin_settlement, returnType=StringType())


def join_with_emd(gdf_poly, sdf, x_colname, y_colname):
    sjoin_udf = create_sjoin_emd(gdf_poly, "EMD_CD")
    res_df = sdf.withColumn("EMD_CD", sjoin_udf(sdf[x_colname], sdf[y_colname]))
    return res_df


def join_with_h3(sdf, x_colname, y_colname, h3_level):
    udf_to_h3 = udf(
        lambda x, y: h3.geo_to_h3(float(x), float(y), h3_level), returnType=StringType()
    )
    res_h3 = sdf.withColumn("h3", udf_to_h3(sdf[y_colname], sdf[x_colname]))
    return res_h3


def join_with_table(gdf_poly, sdf, table_df, x_colname, y_colname):
    temp_df = join_with_emd(gdf_poly, sdf, x_colname, y_colname)
    table_df = table_df.dropDuplicates(["bupjungdong_code"])
    res_df = temp_df.join(
        table_df, [temp_df.EMD_CD == table_df.bupjungdong_code], how="left_outer"
    )

    return res_df
    # .select(temp_df.EMD_CD, table_df.sido).show()

    # res_df.show()
