from pyspark.sql.types import *
from pyspark.sql import SparkSession
import sparkplus
import os
from dotenv import load_dotenv
import geopandas as gpd
from py_log import logger

load_dotenv()


def load_shp_from_s3(bucket, key):
    return gpd.read_parquet(f"s3://{bucket}/{key}")


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
        "integrated_address_daegu",
    ]

    for table in table_list:
        name = table
        globals()[name] = db_table_to_df(spark, table)
    return globals()


spark = SparkSession.builder.appName("Spark App").getOrCreate()

# Load csv file
logger.debug("Loading csv...")
origin = spark.read.csv("s3://sparkplus-core/resource/data/daegu_streetlight.csv")
logger.debug("Loading complete.")

# Clear data
daegu = origin.drop("_c1")
daegu = daegu.where("_c0 > 10000")
custom = sparkplus.CustomDataFrame(daegu, "_c3", "_c2")

# Load parquet file
logger.debug("Loading parquet...")
shp_df = gpd.read_parquet("s3://sparkplus-core/resource/LSMD/Daegu.parquet")
logger.debug("Loading complete...")

# Load table from Database
logger.debug("Loading db...")
db_dict = load_table(spark)
logger.debug("Loading complete...")

result = custom.join_with_table(shp_df, db_dict["integrated_address_daegu"])
result.show()
