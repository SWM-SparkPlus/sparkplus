from typing import List
from pyspark.sql.session import SparkSession

class SPDataFrame(object):
    """
    요약
    -------
    `SPDataFrame` 은 Spark DataFrame를 확장하며, 한국 주소체계를 더 쉽게 다룰 수 있도록 다양한 기능을 제공합니다.
    """

    # TODO: tablenames를 List[ESido] 또는 테이블명을 가진 List 형태로 변경
    @classmethod
    def get_db_df_by_tablenames(
        cls, sparkSession: SparkSession, tablenames: List[str], **kwargs
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

        # If SparkConf doesn't have MySQL driver, raise `ValueError`
        jdbc_driver_flag = False
        for (conf_key, conf_val) in sess_conf:
            if conf_key == "spark.driver.extraClassPath" and conf_val.__contains__(
                "mysql"
            ):
                jdbc_driver_flag = True
                break

        if not jdbc_driver_flag:
            raise ValueError(
                "[SPARKPLUS_EXTRA_JAR_ERR] "
                "Your Spark session seems that it doesn't contains extra class path for mysql connector. "
                "Please specify to use SparkPlus package properly.\n\n"
                "$ spark-submit <your-spark-app> --extra-class-path <mysql-jar-path>"
                "\n\nIn programming way, set spark conf like\n\n"
                ">>> ss = SparkSession.builder.config('spark.driver.extraClassPath', MYSQL_JAR_PATH)\n\n"
                "Check https://spark.apache.org/docs/latest/configuration.html for detail."
            )

        ss_read = sparkSession.read.format("jdbc")

        # set DB options such as driver, url, user, password
        for opt_key, opt_val in kwargs.items():
            ss_read.option(opt_key, opt_val)

        dfs = ss_read.option("dbtable", tablenames.pop()).load()

        while tablenames:
            dfs = dfs.union(ss_read.option("dbtable", tablenames.pop()).load())

        return dfs
