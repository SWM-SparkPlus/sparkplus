import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import encode

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from jobs.conversion import join_with_h3, join_with_emd
from jobs.load_database import load_tables
from package import gis

driver = "com.mysql.cj.jdbc.Driver"
url = "jdbc:mysql://localhost:3306/sparkplus"
user = "sparkplus"
password = "sparkplus"

filepath = "../resource/data/daegu_streetlight.csv"
shp = "../resource/EMD_202101/TL_SCCO_EMD.shp"

if __name__ == "__main__":

    session = SparkSession.builder.appName("demo_app").getOrCreate()
    sc = session.sparkContext
    sc.setLogLevel("ERROR")

    gdf = gis.load_shp(session, shp)
    gdf = gdf.to_crs(4326)

    dataFrameReader = session.read

    my_sdf = dataFrameReader \
        .option("header", True) \
        .format("csv") \
        .load(filepath, encoding='euc-kr')

    emd_df = join_with_emd(gdf, my_sdf, "경도", "위도")
    emd_df.show()

    h3_df = join_with_h3(my_sdf, "경도", "위도", 10)
    h3_df.show()
