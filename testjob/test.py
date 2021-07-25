import sys
import os

import pandas as pd
import mysql.connector

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from dependencies.spark import start_spark

test_spark, *_ = start_spark()
# test_spark = test_spark.read.csv('../train.csv', header=True, inferSchema=True).select(['SUBWAY_ID', 'STATN_ID', 'STATN_NM'])

test_df = test_spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/test_db") \
    .option("driver", "com.mysql.jdbc.Driver").option('dbtable', "Trains") \
    .option("user", "root").option("password", "9315").load()

test_df.show()