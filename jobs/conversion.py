from shapely.geometry import Point, Polygon
from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.sql.types import StructField, StructType, DoubleType

import geopandas as gpd 
import pandas as pd
import h3

"""
def coord_to_dong(spark, gdf, lng, lat):
    addr = gdf[gdf.geometry.contains(Point(lng, lat)) == True]
    addr_drop_geom = addr.drop(columns="geometry")
    sdf = spark.createDataFrame(addr_drop_geom)
    sdf = sdf.select(concat(sdf.EMD_CD, lit("00")).alias('EMD_CD'), 'EMD_ENG_NM', 'EMD_KOR_NM')
    return sdf
"""

def coord_df_to_emd(spark, gdf, sdf, lng_colname, lat_colname):
    pdf = sdf.select('*').toPandas()
    g_df = gpd.GeoDataFrame(pdf, geometry=gpd.points_from_xy(pdf[lng_colname], pdf[lat_colname]))
    li = list()
    for i in g_df.index:
        for j in gdf.index:
            if gdf.geometry[j].contains(g_df.geometry[i]):
                li.append(gdf.EMD_CD[j])
    g_df.insert(len(g_df.columns), "EMD_CD", li)
    g_df = spark.createDataFrame(g_df)
    return g_df

def coord_to_emd(spark, gdf, lng, lat, lng_colname='lng', lat_colname='lat'):
    mySchema = StructType([
        StructField(lng_colname, DoubleType(), True),
        StructField(lat_colname, DoubleType(), True)
    ])
    myRow = Row(lng, lat)
    myDf = spark.createDataFrame([myRow], mySchema)
    result = coord_df_to_emd(spark, gdf, myDf, lng_colname, lat_colname)
    return result

def to_polygon(l):
        return Polygon(h3.h3_to_geo_boundary(l, geo_json=True))

def coord_to_h3(lng, lat, h3_level):
    my_h3 = h3.geo_to_h3(lat, lng, h3_level)
    h3_df = gpd.GeoDataFrame({'h3': [my_h3, my_h3]})
    h3_df['geometry'] = h3_df['h3'].apply(to_polygon)
    h3_df.crs = {'init': 'epsg:4326'}
    return h3_df

def coord_to_jibun(spark, gdf, table_df, lng, lat):
    emd_df = coord_to_emd(spark, gdf, lng, lat).toPandas()
    emd_cd = emd_df.iloc[0]['EMD_CD'] + '00'

    jibun_df = table_df[table_df['bupjungdong_code'] == emd_cd].toPandas()
    return jibun_df

def coord_to_roadname(spark, gdf, table_jibun, table_roadname, tabel_roadname_code, lng, lat):
    jibun_df = coord_to_jibun(spark, gdf, table_jibun, lng, lat)
    manage_number = jibun_df.iloc[0]['manage_number']
    roadname_code_df = table_roadname[table_roadname['manage_number'] == manage_number].toPandas()
    roadname_code = roadname_code_df.iloc[0]['roadname_code']
    result = tabel_roadname_code[tabel_roadname_code['roadname_code'] == roadname_code]
    return result