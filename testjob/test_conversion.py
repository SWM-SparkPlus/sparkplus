import sys
import os

import pandas as pd
import geopandas as gpd 

from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, DoubleType
from shapely.geometry import Polygon

import h3
import folium

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from dependencies.spark import start_spark
from jobs.table_to_df import create_df
from package import gis
from jobs.conversion import coord_df_to_emd, coord_to_emd, coord_to_h3, coord_to_jibun, coord_to_roadname

table_list = [
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

driver = "com.mysql.cj.jdbc.Driver"
url = "jdbc:mysql://localhost:3306/sparkplus"
user = "root"
password = "9315"

filepath = "../resource/LI_202101/TL_SCCO_LI.shp"
# table_name = "roadname_address_seoul"


spark, *_ = start_spark()

for table in table_list:
    name = table + "_df"
    # globals()[name] = create_df(spark, table)
    globals()[name] = spark.read.format("jdbc") \
                            .option("driver", "com.mysql.cj.jdbc.Driver") \
                            .option("url", url) \
                            .option("dbtable", table) \
                            .option("user", user) \
                            .option("password", password) \
                            .load()

gdf = gis.load_shp(spark, "../resource/EMD_202101/TL_SCCO_EMD.shp")
gdf = gdf.to_crs(4326)

hongdae_lat = 37.55743
hongdae_lng = 126.92580

# result = coord_to_emd(spark, gdf, hongdae_lng, hongdae_lat)

# result.show()

mySchema = StructType([
        StructField('x', DoubleType(), True),
        StructField('y', DoubleType(), True)
    ])
myRow = Row(hongdae_lng, hongdae_lat)
myDf = spark.createDataFrame([myRow], mySchema)

h3_df = coord_to_h3(hongdae_lng, hongdae_lat, 10)
print(h3_df)

jibun_df = coord_to_jibun(spark, gdf, jibun_address_seoul_df, hongdae_lng, hongdae_lat)

roadname_df = coord_to_roadname(spark, gdf, jibun_address_seoul_df, roadname_address_seoul_df, roadname_code_df, hongdae_lng, hongdae_lat)