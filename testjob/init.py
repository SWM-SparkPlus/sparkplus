"""
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark-Plus").getOrCreate()

df_pyspark = spark.read.csv('../train.csv', header=True, inferSchema=True)

df_pyspark = df_pyspark.select(['SUBWAY_ID', 'STATN_ID', 'STATN_NM'])

df_pyspark.describe().show()
"""