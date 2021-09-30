import os
import sys

import geopandas as gpd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))))

from jobs.sparkplus import CustomDataFrame
from pyspark.sql import SparkSession
from sparkplus.dependencies.spark import start_spark


driver = "com.mysql.cj.jdbc.Driver"
url = "jdbc:mysql://localhost:3306/sparkplus"
user = "sparkplus"
password = "sparkplus"

shp_path = "../resource/shp/LSMD_CONT_LDREG_27_202109.shp"
data_path = "../resource/data/daegu_streetlight.csv"

"""
session = (
        SparkSession.builder.appName("demo_app")
        .config(
            "spark.driver.extraClassPath",
            "/usr/lib/spark/jars/mysql-connector-java-8.0.26.jar",
        )
        .getOrCreate()
    )
"""

session, _ = start_spark()

dataFrameReader = session.read

gdf = gpd.read_file(shp_path, encoding='euc-kr')
gdf.crs = "epsg:5174"
gdf = gdf.to_crs(epsg=4326)

my_sdf = (
    dataFrameReader.option("header", True)
        .format("csv")
        .load(data_path, encoding="euc-kr")
)

df = CustomDataFrame(my_sdf)

res_df = df.coord_to_pnu(gdf, '경도', '위도')

res_df.show()
