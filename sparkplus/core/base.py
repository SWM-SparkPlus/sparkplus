from typing import List
from pyspark.sql.session import SparkSession
from geopandas.geodataframe import GeoDataFrame
from pyspark.sql.functions import col, column, lit, udf, pandas_udf
from pyspark.sql import DataFrame
from pyspark.sql import types

import geopandas as gpd
import h3


class SPDataFrame(object):
    """
    요약
    -------
    `SPDataFrame` 은 Spark DataFrame를 확장하며, 한국 주소체계를 더 쉽게 다룰 수 있도록 다양한 기능을 제공합니다.

    사용법
    -----
    >>> import sparkplus
    >>> df = spark.read...
    >>> spdf = sparkplus.SPDataFrame(df) # Spark DataFrame 을 파라미터로 받아 사용합니다.
    """

    def __init__(self, dataFrame: DataFrame):
        self._df = dataFrame

    def table_pnu_df_join(
        self,
        table_df: DataFrame,
        pnu_df: GeoDataFrame,
        drop_dup_bupjungdong_code: bool = True,
    ):
        """
        Args:
            table_df (DataFrame): 데이터베이스로부터 생성한 Spark DataFrame
            pnu_df (GeoDataFrame): shp Parquet로부터 생성한 GeoDataFrame
            drop_dups (bool, optional): 법정동코드 중복 제거 여부. Defaults to True.

        Returns:
            `pyspark.sql.DataFrame`
        """
        # 테이블 및 파케이 데이터프레임이 메모리에 상주하지 않다면 에러 발생
        if not isinstance(table_df, DataFrame) or not isinstance(pnu_df, GeoDataFrame):
            raise ValueError

        # 데이터베이스에 법정동 코드가 중복되어 중복 제거
        if drop_dup_bupjungdong_code:
            table_df = table_df.dropDuplicates(["bupjungdong_code"])

        return pnu_df.join(
            table_df, [pnu_df.PNU[0:10] == table_df.bupjungdong_code], how="left_outer"
        ).drop_duplicates(["PNU"])

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

        for tablename in tablenames:
            dfs = dfs.union(ss_read.option("dbtable", tablename).load())

        return dfs

    # def coord_to_h3(self, h3_level: int):
    #     udf_to_h3 = udf(
    #         lambda x, y: h3.geo_to_h3(float(x), float(y), h3_level),
    #         returnType=types.StringType(),
    #     )

    #     res_h3 = self._df.withColumn(
    #         "h3",
    #         udf_to_h3(self._df[self._y_colname], self._df[self._x_colname]),
    #     )
    #     return res_h3

    # def coord_to_pnu(self):
    #     return self.pnu_df

    # def coord_to_zipcode(self):
    #     joined_df = self.joined_df.select("PNU", "zipcode")
    #     res_df = self.pnu_df.join(joined_df, "PNU", "leftouter")
    #     return res_df

    # def coord_to_emd(self):
    #     joined_df = self.joined_df.select("PNU", "bupjungdong_code")
    #     res_df = self.pnu_df.join(joined_df, "PNU", "leftouter")
    #     return res_df

    # def coord_to_doromyoung(self):
    #     joined_df = self.joined_df.select(
    #         "PNU",
    #         "sido",
    #         "sigungu",
    #         "roadname",
    #         "is_basement",
    #         "building_primary_number",
    #         "building_secondary_number",
    #         "bupjungdong_code",
    #     )
    #     res_df = self.pnu_df.join(joined_df, "PNU", "leftouter")
    #     return res_df

    # def coord_to_jibun(self):
    #     joined_df = self.joined_df.select(
    #         "PNU",
    #         "sido",
    #         "sigungu",
    #         "eupmyeondong",
    #         "bupjungli",
    #         "jibun_primary_number",
    #         "jibun_secondary_number",
    #     )
    #     res_df = self.pnu_df.join(joined_df, "PNU", "leftouter")
    #     return res_df

    """
	def coord_to_zipcode2(self):
		pnu_df = self.pnu_df
		res_df = pnu_df.withColumn('zipcode', self.joined_df['zipcode'])
		return res_df
	"""

    """
	def create_sjoin_pnu(self, join_column_name):
		def sjoin_settlement(x, y):
			gdf_temp = gpd.GeoDataFrame(
				data=[[x] for x in range(len(x))], geometry=gpd.points_from_xy(x, y)
			).set_crs(epsg=4326, inplace=True)
			settlement = gpd.sjoin(gdf_temp, self._gdf, how='left', op='within')
			settlement = settlement.drop_duplicates(subset='geometry')

			return (
				settlement.agg({"PNU": lambda x: str(x)})
				.reset_index()
				.loc[:, join_column_name]
				.astype("str")
			)
		return pandas_udf(sjoin_settlement, returnType=types.StringType())

	def join_with_table(self):
		temp_df = self.coord_to_pnu()
		table_df = self._table.dropDuplicates(["bupjungdong_code"])
		res_df = temp_df.join(
			table_df, [temp_df.PNU[0:10] == table_df.bupjungdong_code], how='left_outer'
		)

		return res_df
	"""

    """
	def coord_to_pnu(self):
		sjoin_udf = self.create_sjoin_pnu("PNU")
		res_df = self._origin_df.withColumn("PNU", sjoin_udf(self._origin_df[self._x_colname], self._origin_df[self._y_colname]))

		return res_df
	"""


"""
class AddressTo
	def address_to_h3(self, )

	def coord_to_zip(self, gdf):
		self.origin_df = self

	def coord_to_emd(self, x_colname, y_colname):
		self.origin_df = self

	def coord_to_address(self):
		self.origin_df = self

	def address_to_h3(self):
		self.origin_df = self

	def address_to_zip(self):
		self.origin_df = self

	def address_to_emd(self):
		self.origin_df = self

	def address_to_coord(self):
		self.origin_df = self
"""
