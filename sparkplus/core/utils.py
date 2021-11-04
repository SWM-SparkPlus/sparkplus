import geopandas as gpd
from typing import List, Union
from pyspark.sql.session import SparkSession

def load_tables(
    sparkSession: SparkSession, tablenames: Union[str, List[str]], **kwargs
):
    """
    Summary
    -------
    테이블명을 기반으로 Spark DataFrame을 반환합니다.

    Parameter
    ----
    sparkSession: Active Spark Session
    tablenames: DataFrame으로 만들 테이블명
    **kwargs: `driver`, `url`, `user`, `password`

    Raises:
        ValueError

    Returns:
        `DataFrame`s from database


    Usage
    -----
    >>> import SPDataFrame
    >>> ss = SparkSession.builder.getOrCreate()
    >>> tablenames = ['integrated_address_seoul', 'integrated_address_incheon', 'integrated_address_gyeonggi']
    >>> table_dfs = SPDataFrame(ss, tablenames,
                        driver='com.mysql.cj.jdbc.Driver',
                        url='jdbc:mysql://localhost:3306/sparkplus',
                        user='root',
                        password='password'
                        )
    >>> table_dfs.select('roadname_code', 'sido', 'sigungu', 'eupmyeondong').show()
    +-------------+----------+-------------+------------+
    |roadname_code|      sido|      sigungu|eupmyeondong|
    +-------------+----------+-------------+------------+
    | 261103125011|부산광역시|         중구|      영주동|
    | 261104006006|부산광역시|         중구|      영주동|
    | 261104006006|부산광역시|         중구|      영주동|
    | 261104006006|부산광역시|         중구|      영주동|
    | 261103125011|부산광역시|         중구|      영주동|
    | 111104100289|서울특별시|       종로구|      청운동|
    | 111104100289|서울특별시|       종로구|      청운동|
    | 111103100014|서울특별시|       종로구|      청운동|
    | 111104100289|서울특별시|       종로구|      청운동|
    | 111104100289|서울특별시|       종로구|      청운동|
    | 411114322017|    경기도|수원시 장안구|      파장동|
    | 411114322017|    경기도|수원시 장안구|      파장동|
    | 411114322017|    경기도|수원시 장안구|      파장동|
    | 411114322017|    경기도|수원시 장안구|      파장동|
    | 411114322017|    경기도|수원시 장안구|      파장동|
    +-------------+----------+-------------+------------+
    """
    sess_conf = sparkSession.sparkContext.getConf().getAll()

    # If SparkConf doesn't contain MySQL connector, raise `ValueError`
    jdbc_driver_flag = False

    # If you use `spark.jars.packages`, value should like `mysql:mysql-connector-java:YOUR_MYSQL_VERSION`
    available_configs = [
        "spark.jars",
        "spark.driver.extraClassPath",
        "spark.jars.packages",
    ]

    for (conf_key, conf_val) in sess_conf:
        if conf_key in available_configs and conf_val.__contains__("mysql"):
            jdbc_driver_flag = True
            break

    if not jdbc_driver_flag:
        raise ValueError(
            "[SPARKPLUS_MYSQL_CONNECTOR_ERR] "
            "Your spark session seems like it doesn't contains mysql-connector-java path to connect mysql database. "
            "Please specify it to use SparkPlus package properly.\n\n"
            "$ spark-submit <your-spark-app> --jars <mysql-jar-path>\n\n"
            "In programming way, if you have mysql-connector jar file locally, set spark configuration like\n\n"
            ">>> ss = SparkSession.builder.config('spark.jars', MYSQL_JAR_PATH)\n\n"
            "or if you don't,\n\n"
            ">>> ss = SparkSession.builder.config('spark.jars.packages', 'mysql:mysql-connector-java:YOUR_MYSQL_VERSION')\n\n"
            "Check https://spark.apache.org/docs/latest/configuration.html for detail."
        )

    ss_read = sparkSession.read.format("jdbc")

    # set DB options such as driver, url, user, password
    for opt_key, opt_val in kwargs.items():
        ss_read.option(opt_key, opt_val)

    if isinstance(tablenames, str):
        return ss_read.option("dbtable", tablenames).load()
    else:
        dfs = ss_read.option("dbtable", tablenames.pop()).load()

        while tablenames:
            dfs = dfs.union(ss_read.option("dbtable", tablenames.pop()).load())

        return dfs

def load_gdf(shp_path , epsg):
    gdf = gpd.read_file(shp_path, encoding="euc-kr")
    gdf.crs = f'epsg:{epsg}'
    gdf = gdf.to_crs(epsg=4326)

    return gdf