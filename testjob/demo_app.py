import os
import sys

from shapely.geometry import Polygon
import folium
from pyspark.sql import SparkSession
from pyspark.sql.functions import encode
import pandas as pd
import geopandas as gpd
import h3

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from jobs.conversion import join_with_h3, join_with_emd, join_with_table
from jobs.load_database import load_tables
from package import gis

driver = "com.mysql.cj.jdbc.Driver"
url = "jdbc:mysql://172.31.63.49:3306/sparkplus"
user = "sparkplus"
password = "sparkplus"

filepath = "/home/hadoop/spark-plugin/resource/data/daegu_streetlight.csv"
shp = "/home/hadoop/spark-plugin/resource/EMD_202101/TL_SCCO_EMD.shp"

if __name__ == "__main__":

    session = SparkSession \
                .builder \
                .appName("demo_app") \
                .config("spark.driver.extraClassPath", "/usr/lib/spark/jars/mysql-connector-java-8.0.26.jar") \
                .getOrCreate()
    # session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    # session.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 20000)

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
    
    tdf = pd.read_csv(filepath, encoding='euc-kr')

    tdf2 = tdf.iloc[:][10054:10059]
    tdf2 = session.createDataFrame(tdf2)
    print("tdf2")
    tdf2.show()
    tdf = tdf.iloc[:][10057:10059]
    tdf = session.createDataFrame(tdf)
    tdf.show()
    tdf = join_with_emd(gdf, tdf, '경도', '위도')
    tdf.show()

    tdf2 = join_with_emd(gdf, tdf2, '경도', '위도')

    """
    h3_df = join_with_h3(my_sdf, "경도", "위도", 10)
    h3_df.show()
    """
    table_df = load_tables(session, url, user, password, 'daegu')
    table_df.show()

    res_df = join_with_table(gdf, tdf, table_df, '경도', '위도')
    # res_df.show()
    res_df.show()
    """
    res2_df = join_with_table(gdf, tdf2, table_df, '경도', '위도')
    res2_df.show()
    """
    """
    Result vector from pandas_udf was not the required lengt
    def to_polygon(l):
	    return Polygon(h3.h3_to_geo_boundary(l, geo_json=True))

    temp = [35.8734, 128.6103]

    gdf_h3 = h3_df.toPandas()
    gdf_h3 = gpd.GeoDataFrame(gdf_h3)
    gdf_h3['geometry'] = gdf_h3['h3'].apply(to_polygon)
    gdf_h3.crs = {'init': 'epsg:4326'}

    m =folium.Map(temp, zoom_start=14)
    folium.GeoJson(gdf_h3).add_to(m)

    m.save('daegu1.html')
    """
