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

        Raises:
            `ValueError`

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

    def coord_to_h3(self, h3_level: int):
        udf_to_h3 = udf(
            lambda x, y: h3.geo_to_h3(float(x), float(y), h3_level),
            returnType=types.StringType(),
        )

        res_h3 = self._df.withColumn(
            "h3",
            udf_to_h3(self._df[self._y_colname], self._df[self._x_colname]),
        )
        return res_h3

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

    def join_with_table(self):
        return self.joined_df

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
