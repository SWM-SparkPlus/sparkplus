import sys
import os

import pandas as pd
import mysql.connector

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from dependencies.spark import start_spark
from jobs.with_geopandas import geopandas_df_to_spark_for_points

def download_file(url):
    filename = url.split('/')[-1]
    response = urllib.request.urlopen(url)
    content = response.read()

spark, *_ = start_spark()
# test_spark = test_spark.read.csv('../train.csv', header=True, inferSchema=True).select(['SUBWAY_ID', 'STATN_ID', 'STATN_NM'])

# Spark with GeoPandas
sdf = geopandas_df_to_spark_for_points(spark, gdf)
display(sdf)
# Spark with MySQL
'''
test_df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/test_db") \
    .option("driver", "com.mysql.jdbc.Driver").option('dbtable', "Trains") \
    .option("user", "root").option("password", "9315").load()

test_df.show()
'''