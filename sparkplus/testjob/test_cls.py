import os
import sys

import geopandas as gpd
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))))

from sparkplus.core.sparkplus import CoordDataFrame
from sparkplus.jobs.load_database import load_tables
from pyspark.sql import SparkSession
from sparkplus.dependencies.spark import start_spark
from sparkplus.core.py_log import logger

load_dotenv()

driver = "com.mysql.cj.jdbc.Driver"
url = "jdbc:mysql://ec2-3-35-104-222.ap-northeast-2.compute.amazonaws.com:3306/sparkplus"
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

# Spark Session을 연다
session, _ = start_spark()
dataFrameReader = session.read

logger.debug('read_shp')
# shp파일을 GDF로 불러오고 crs를 세팅한다.
gdf = gpd.read_file(shp_path, encoding='euc-kr')
gdf.crs = "epsg:5174"
gdf = gdf.to_crs(epsg=4326)
logger.debug('complete read shp')

# 데이터 df를 불러온다.
logger.debug('read dataframe')
my_sdf = (
    dataFrameReader.option("header", True)
        .format("csv")
        .load(data_path, encoding="euc-kr")
)
my_sdf.show()
print('my_sdf: ', my_sdf.count())
logger.debug('complete dataframe')

# 데이터베이스에서 테이블을 불러온다.
logger.debug('load_tables')
table_df = load_tables(session, url, user, password, 'daegu')
table_df.show()
logger.debug('complete load_tables')
# 커스텀데이터프레임을 만든다.
logger.debug('create custom df')
df = CoordDataFrame(my_sdf, gdf, table_df, '경도', '위도')
logger.debug('complete custom df')
# 기존 데이터 df와 PNU 매칭한다.
logger.debug('coord_to_pnu')
pnu_df = df.coord_to_pnu()

print("pnu_df: ", pnu_df.count())
pnu_df.show()

logger.debug('complete coord_to_pnu')

"""
logger.debug('join with pnu')
res_df = df.coord_to_pnu(gdf, '경도', '위도')
res_df.show()
logger.debug('complete join with pnu')
"""


# 기존 데이터 df와 테이블을 조인한다. (PNU => bupjungdong 매칭)
logger.debug('join_with_table')
res_df = df.join_with_table()
print('joined_df: ', res_df.count())
res_df.show()
logger.debug('complete join_with_tables')

logger.debug('h3_df')
h3_df = df.coord_to_h3(10)
print('h3_df: ', h3_df.count())
h3_df.show()
logger.debug('complete h3_df')

logger.debug('select zipcode columns')
zipcode_df = df.coord_to_zipcode()
print('zipcode_df: ', zipcode_df.count())

zipcode_df.show()
logger.debug('complete select zip columns')


logger.debug('select emd columns')
emd_df = df.coord_to_emd()
print('emd_df: ', emd_df.count())

emd_df.show()
logger.debug('complete select emd columns')


logger.debug('select doromyoung columns')
doro_df = df.coord_to_roadname()
print('doro_df: ', doro_df.count())

doro_df.show()
logger.debug('complete select doromyoung columns')



logger.debug('select jibun columns')
jibun_df = df.coord_to_jibun()
print('jibun_df: ', jibun_df.count())

jibun_df.show()
logger.debug('complete select jibun columns')
