import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from dependencies.spark import start_spark

test_spark, *_ = start_spark()
test_spark.read.csv('../train.csv', header=True, inferSchema=True).select(['SUBWAY_ID', 'STATN_ID', 'STATN_NM']).show()