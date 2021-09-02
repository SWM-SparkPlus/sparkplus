import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from dependencies.spark import start_spark
from jobs.table_to_df import create_df
from jobs.load_database import load_tables

driver = "com.mysql.cj.jdbc.Driver"
url = "jdbc:mysql://localhost:3306/sparkplus"
user = "root"
password = "9315"

spark, *_ = start_spark()

result = load_tables(spark, url, user, password, opt=0)

result.show()
