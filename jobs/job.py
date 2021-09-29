from pyspark.sql.types import *
from pyspark.sql import SparkSession
import sparkplus
import os
from dotenv import load_dotenv
import geopandas as gpd

load_dotenv()

spark = SparkSession.builder.appName("Spark App").getOrCreate()
origin = spark.read.csv("../resource/daegu_streetlight.csv")
daegu = origin.drop("_c1")
daegu = daegu.where("_c0 > 10000")
cdf = sparkplus.CustomDataFrame(daegu)
cdf.origin_df.show()

# table_name = "integrated_address_daejeon"
# db_df = (
#         spark.read.format("jdbc")
#         .option("driver", os.getenv("DB_DRIVER"))
#         .option("url", os.getenv("DB_URL"))
#         .option("dbtable", table_name)
#         .option("user", os.getenv("DB_USER"))
#         .option("password", os.getenv("DB_PASSWORD"))
#         .load()
#     )
# db_df.coalesce(1).write.csv("result")
# cdf.coord_to_h3("_c2","_c3",10).show()
# gdf = gpd.read_parquet("daegu.parquet")
# print(gdf)
gdf = gpd.read_file("/home/hadoop/spark-plugin/resource/lsmd/LSMD_CONT_LDREG_11_202109.shp", encoding='euc-kr')
gdf = gdf.set_crs(5174)
gdf = gdf.to_crs(4326)
gpd.read
gdf.to_parquet("seoul.parquet")