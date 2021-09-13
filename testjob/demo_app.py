import os
import sys

from shapely.geometry import Polygon
import folium
from pyspark.sql import SparkSession
from pyspark.sql.functions import encode
import geopandas as gpd
import h3

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from jobs.conversion import join_with_h3, join_with_emd
from jobs.load_database import load_tables
from package import gis

driver = "com.mysql.cj.jdbc.Driver"
url = "jdbc:mysql://localhost:3306/sparkplus"
user = "sparkplus"
password = "sparkplus"

filepath = "/home/hadoop/spark-plugin/resource/data/daegu_streetlight.csv"
shp = "/home/hadoop/spark-plugin/resource/EMD_202101/TL_SCCO_EMD.shp"

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

    """
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
