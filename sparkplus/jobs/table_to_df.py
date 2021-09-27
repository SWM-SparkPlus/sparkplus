def create_df(spark, table):
    
    sdf = spark.read.format("jdbc")\
        .option("url", "jdbc:mysql://localhost:3306/sparkplus") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option('dbtable', table) \
        .option("user", "root") \
        .option("password", "9315") \
        .load()

    return sdf