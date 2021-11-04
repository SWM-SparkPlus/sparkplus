from geopandas.geodataframe import GeoDataFrame
from pyspark.sql.functions import lit, udf, pandas_udf
from pyspark.sql import DataFrame
from pyspark.sql.types import *

import geopandas as gpd
import h3


def create_sjoin_pnu(gdf, join_column_name):
    def sjoin_settlement(x, y):
        gdf_temp = gpd.GeoDataFrame(
            data=[[x] for x in range(len(x))], geometry=gpd.points_from_xy(x, y)
        ).set_crs(epsg=4326, inplace=True)
        settlement = gpd.sjoin(gdf_temp, gdf, how="left", predicate="within")
        settlement = settlement.drop_duplicates(subset="geometry")

        return (
            settlement.agg({"PNU": lambda x: str(x)})
            .reset_index()
            .loc[:, join_column_name]
            .astype("str")
        )

    return pandas_udf(sjoin_settlement, returnType=StringType())


def _coord_to_pnu(origin_df, gdf, x_colname, y_colname):
    sjoin_udf = create_sjoin_pnu(gdf, "PNU")
    res_df = origin_df.withColumn(
        "PNU", sjoin_udf(origin_df[x_colname], origin_df[y_colname])
    )
    return res_df


def _join_with_table(table_df, pnu_df):
    # temp_df = self.coord_to_pnu()
    table_df = table_df.dropDuplicates(["bupjungdong_code"])
    res_df = pnu_df.join(
        table_df, [pnu_df.PNU[0:10] == table_df.bupjungdong_code], how="left_outer"
    )
    # res_df = res_df.dropDuplicates(['PNU'])

    return res_df


@udf(StringType())
def get_fullname(a, b, c, d):
    if a == None and b == None and c == None and d == None:
        return None

    if a == None:
        a = ""
    if b == None:
        b = ""
    if c == None:
        c = ""
    if d == None:
        d = ""

    res = str(a) + " " + str(b) + " " + str(c) + " " + str(d) + " " 

    return res

class CoordDataFrame(DataFrame):
    """
    Summary
    -------
    위경도 좌표가 포함된 Spark DataFrame에 법정읍면동, h3, 우편번호 정보를 추가합니다.

    Args:
            origin_sdf (Spark DataFrame):  위경도 좌표가 포함된 원본 Spark DataFrame
            gdf (GeoDataFrame): shp Parquet으로부터 생성한 GeoDataFrame
            tdf (Spark DataFrame): 데이터베이스로부터 생성한 Spark DataFrame
            x_colname (String): 원본 Spark DataFrame의 경도 컬럼 이름
            y_colname (String): 원본 Spark DataFrame의 위도 컬럼 이름

    Usage
    -------
    >>> from sparkplus.core.sparkplus import CoordDataFrame
    >>> df = CoordDataFrame(origin_sdf, gdf, tdf, x_colname, y_colname)
    """

    def __init__(self, origin_sdf, gdf, tdf, x_colname, y_colname):
        self._origin_sdf = origin_sdf
        self._gdf = gdf
        self._tdf = tdf
        self._x_colname = x_colname
        self._y_colname = y_colname

        self.pnu_df = _coord_to_pnu(origin_sdf, gdf, x_colname, y_colname).cache()
        self.joined_df = _join_with_table(tdf, self.pnu_df).cache()

    def add_h3(self, h3_level):
        """
        Summary
        -------
        위경도 좌표가 포함된 원본 Spark DataFrame에 h3 정보를 추가합니다.

        Args:
                h3_level (Int): 추가하고자 하는 h3 level

        Usage
        -------
        >>> from sparkplus.core.sparkplus import CoordDataFrame
        >>> df = CoordDataFrame(origin_sdf, gdf, tdf, 'lon', 'lat')
        >>> res_df = df.coord_to_h3(10)

        Examples
        -------
        >>> origin_sdf.show()
        +----------+--------+-----------+-----------+
        |   가로등번호|  관할구청|        위도|        경도|
        +----------+--------+-----------+-----------+
        |   1001001|     중구|35.87343028|128.6103158|
        |   1001002|     중구|35.87334197|128.6099071|
        |   1001003|     중구|35.87327842|128.6096135|
        +----------+--------+-----------+-----------+

        >>> df = CoordDataFrame(origin_sdf, gdf, tdf, '경도', '위도')
        >>> res_df = df.coord_to_h3(10)
        >>> res_df.show()
        +----------+--------+-----------+-----------+---------------+
        |	가로등번호|	 관할구청|        위도|        경도|             h3|
        +----------+--------+-----------+-----------+---------------+
        |   1001001|     중구|35.87343028|128.6103158|8a30c190311ffff|
        |   1001002|     중구|35.87334197|128.6099071|8a30c190311ffff|
        |   1001003|     중구|35.87327842|128.6096135|8a30c19031affff|
        +----------+--------+-----------+-----------+---------------+
        """
        udf_to_h3 = udf(
            lambda x, y: h3.geo_to_h3(float(x), float(y), h3_level),
            returnType=StringType(),
        )

        res_h3 = self._origin_sdf.withColumn(
            "h3",
            udf_to_h3(
                self._origin_sdf[self._y_colname], self._origin_sdf[self._x_colname]
            ),
        )
        return res_h3

    def add_pnu(self):
        """
        Summary
        -------
        위경도 좌표가 포함된 원본 Spark DataFrame에 pnu 정보를 추가합니다.

        Usage
        -------
        >>> from sparkplus.core.sparkplus import CoordDataFrame
        >>> df = CoordDataFrame(origin_sdf, gdf, tdf, 'lon', 'lat')
        >>> res_df = df.coord_to_pnu()

        Example
        -------
        >>> orgin_sdf.show()
        +----------+--------+-----------+-----------+
        |   가로등번호|  관할구청|        위도|        경도|
        +----------+--------+-----------+-----------+
        |   1001001|     중구|35.87343028|128.6103158|
        |   1001002|     중구|35.87334197|128.6099071|
        |   1001003|     중구|35.87327842|128.6096135|
        +----------+--------+-----------+-----------+

        >>> df = CoordDataFrame(origin_sdf, gdf, tdf, '경도', '위도')
        >>> res_df = df.coord_to_pnu()
        >>> res_df.show()
        +----------+--------+-----------+-----------+-------------------+
        |	가로등번호|	 관할구청|        위도|        경도|                PNU|
        +----------+--------+-----------+-----------+-------------------+
        |   1001001|     중구|35.87343028|128.6103158|2711010300103670054|
        |   1001002|     중구|35.87334197|128.6099071|2711010300103670054|
        |   1001003|     중구|35.87327842|128.6096135|2711010300103670054|
        +----------+--------+-----------+-----------+-------------------+
        """
        return self.pnu_df

    def add_zipcode(self):
        """
        Summary
        -------
        위경도 좌표가 포함된 원본 Spark DataFrame에 우편번호 정보를 추가합니다.

        Usage
        -------
        >>> from sparkplus.core.sparkplus import CoordDataFrame
        >>> df = CoordDataFrame(origin_sdf, gdf, tdf, 'lon', 'lat')
        >>> res_df = df.coord_to_zipcode()

        Example
        -------
        >>> origin_sdf.show()
        +----------+--------+-----------+-----------+
        |   가로등번호|  관할구청|        위도|        경도|
        +----------+--------+-----------+-----------+
        |   1001001|     중구|35.87343028|128.6103158|
        |   1001002|     중구|35.87334197|128.6099071|
        |   1001003|     중구|35.87327842|128.6096135|
        +----------+--------+-----------+-----------+

        >>> df = CoordDataFrame(origin_sdf, gdf, tdf, '경도', '위도')
        >>> res_df = df.coord_to_zipcode()
        >>> res_df.show()
        +----------+--------+-----------+-----------+-------+
        |	가로등번호|	 관할구청|        위도|        경도|zipcode|
        +----------+--------+-----------+-----------+-------+
        |   8155012|   달성군|35.64103224|128.4106523|  43013|
        |   8071024|   달성군|35.66091032|128.4159519|  43006|
        |   8213007|   달성군| 35.6320721|128.4175234|  43013|
        +----------+--------+-----------+-----------+-------+

        """
        joined_df = self.joined_df.select("PNU", "zipcode")
        res_df = self.pnu_df.join(joined_df, "PNU", "leftouter").drop("PNU")
        res_df = res_df.dropDuplicates([self._x_colname, self._y_colname])
        return res_df

    def add_bupjungdong(self):
        """
        Summary
        -------
        위경도 좌표가 포함된 원본 Spark DataFrame에 법정읍면동 코드 정보를 추가합니다.

        Usage
        -------
        >>> from sparkplus.core.sparkplus import CoordDataFrame
        >>> df = CoordDataFrame(origin_sdf, gdf, tdf, 'lon', 'lat')
        >>> res_df = df.coord_to_emd()

        Example
        -------
        >>> origin_sdf.show()
        +----------+--------+-----------+-----------+
        |   가로등번호|  관할구청|        위도|        경도|
        +----------+--------+-----------+-----------+
        |   1001001|     중구|35.87343028|128.6103158|
        |   1001002|     중구|35.87334197|128.6099071|
        |   1001003|     중구|35.87327842|128.6096135|
        +----------+--------+-----------+-----------+

        >>> df = CoordDataFrame(origin_sdf, gdf, tdf, '경도', '위도')
        >>> res_df = df.coord_to_emd()
        >>> res_df.show()
        +----------+--------+-----------+-----------+----------------+
        |	가로등번호|	 관할구청|        위도|        경도|bupjungdong_code|
        +----------+--------+-----------+-----------+----------------+
        |   1001001|     중구|35.87343028|128.6103158|      2711010300|
        |   1001002|     중구|35.87334197|128.6099071|      2711010300|
        |   1001003|     중구|35.87327842|128.6096135|      2711010300|
        +----------+--------+-----------+-----------+----------------+
        """
        joined_df = self.joined_df.select("PNU", "bupjungdong_code")
        res_df = self.pnu_df.join(joined_df, "PNU", "leftouter").drop("PNU")
        res_df = res_df.dropDuplicates([self._x_colname, self._y_colname])
        return res_df

    def add_roadname(self):
        """
        Summary
        -------
        위경도 좌표가 포함된 원본 Spark DataFrame에 도로명 주소 정보를 추가합니다.

        Usage
        -------
        >>> from sparkplus.core.sparkplus import CoordDataFrame
        >>> df = CoordDataFrame(origin_sdf, gdf, tdf, 'lon', 'lat')
        >>> res_df = df.coord_to_roadname()

        Example
        -------
        >>> origin_sdf.show()
        +----------+--------+-----------+-----------+
        |   가로등번호|  관할구청|        위도|        경도|
        +----------+--------+-----------+-----------+
        |   1001001|     중구|35.87343028|128.6103158|
        |   1001002|     중구|35.87334197|128.6099071|
        |   1001003|     중구|35.87327842|128.6096135|
        +----------+--------+-----------+-----------+

        >>> df = CoordDataFrame(origin_sdf, gdf, tdf, 'lon', 'lat')
        >>> res_df = df.coord_to_roadname()
        >>> res_df.show()
        +----------+--------+-----------+-----------+----------+-------+-------------+------------+---------+-----------+-----------------------+-------------------------+
        |	가로등번호|	 관할구청|        위도|        경도|      sido|sigungu|     roadname|eupmyeondong|bupjungli|is_basement|building_primary_number|building_secondary_number|
        +----------+--------+-----------+-----------+----------+-------+-------------+------------+---------+-----------+-----------------------+-------------------------+
        |   1001001|     중구|35.87343028|128.6103158|	 대구광역시|   중구|   	  동덕로38길|     동인동3가|         |          0|                    100|                        0|
        |   1001002|     중구|35.87334197|128.6099071|	 대구광역시|   중구|   	  동덕로38길|  	  동인동3가|         |          0|                    100|                        0|
        |   1001003|     중구|35.87327842|128.6096135|	 대구광역시|   중구|   	  동덕로38길|  	  동인동3가|         |          0|                    100|                        0|
        +----------+--------+-----------+-----------+----------+-------+-------------+------------+---------+-----------+-----------------------+-------------------------+

        """
        joined_df = self.joined_df.select(
            "PNU",
            "sido",
            "sigungu",
            "roadname",
            "eupmyeondong",
            "bupjungli",
            "is_basement",
            "building_primary_number",
            "building_secondary_number",
        )
        res_df = self.pnu_df.join(joined_df, "PNU", "leftouter").drop("PNU")
        res_df = res_df.dropDuplicates([self._x_colname, self._y_colname])
        return res_df

    def add_roadname_addr(self):
        """
        Summary
        -------
        위경도 좌표가 포함된 원본 Spark DataFrame에 도로명 주소 정보를 추가합니다.

        Usage
        -------
        >>> from sparkplus.core.sparkplus import CoordDataFrame
        >>> df = CoordDataFrame(origin_sdf, gdf, tdf, 'lon', 'lat')
        >>> res_df = df.coord_to_roadname()

        Example
        -------
        >>> origin_sdf.show()
        +----------+--------+-----------+-----------+
        |   가로등번호|  관할구청|        위도|        경도|
        +----------+--------+-----------+-----------+
        |   1001001|     중구|35.87343028|128.6103158|
        |   1001002|     중구|35.87334197|128.6099071|
        |   1001003|     중구|35.87327842|128.6096135|
        +----------+--------+-----------+-----------+

        >>> df = CoordDataFrame(origin_sdf, gdf, tdf, 'lon', 'lat')
        >>> res_df = df.coord_to_roadname()
        >>> res_df.show()
        +----------+--------+-----------+-----------+----------+-------+-------------+------------+---------+-----------+-----------------------+-------------------------+
        |	가로등번호|	 관할구청|        위도|        경도|      sido|sigungu|     roadname|eupmyeondong|bupjungli|is_basement|building_primary_number|building_secondary_number|
        +----------+--------+-----------+-----------+----------+-------+-------------+------------+---------+-----------+-----------------------+-------------------------+
        |   1001001|     중구|35.87343028|128.6103158|	 대구광역시|   중구|   	  동덕로38길|     동인동3가|         |          0|                    100|                        0|
        |   1001002|     중구|35.87334197|128.6099071|	 대구광역시|   중구|   	  동덕로38길|  	  동인동3가|         |          0|                    100|                        0|
        |   1001003|     중구|35.87327842|128.6096135|	 대구광역시|   중구|   	  동덕로38길|  	  동인동3가|         |          0|                    100|                        0|
        +----------+--------+-----------+-----------+----------+-------+-------------+------------+---------+-----------+-----------------------+-------------------------+

        """
        joined_df = self.joined_df.select(
            "PNU",
            "sido",
            "sigungu",
            "roadname",
            "eupmyeondong",
            "bupjungli",
            "is_basement",
            "building_primary_number",
            "building_secondary_number",
        )
        joined_df = joined_df.withColumn(
            "roadname_address",
            get_fullname(
                joined_df["sido"],
                joined_df["sigungu"],
                joined_df["roadname"],
                joined_df["building_primary_number"],
            ),
        )
        res_df = self.pnu_df.join(joined_df, "PNU", "leftouter").drop("PNU")
        res_df = res_df.dropDuplicates([self._x_colname, self._y_colname])
        return res_df

    def add_jibun(self):
        """
        Summary
        -------
        위경도 좌표가 포함된 원본 Spark DataFrame에 지번 주소 정보를 추가합니다.

        Usage
        -------
        >>> from sparkplus.core.sparkplus import CoordDataFrame
        >>> df = CoordDataFrame(origin_sdf, gdf, tdf, 'lon', 'lat')
        >>> res_df = df.coord_to_jibun()

        Example
        -------
        >>> origin_sdf.show()
        +----------+--------+-----------+-----------+
        |   가로등번호|  관할구청|        위도|        경도|
        +----------+--------+-----------+-----------+
        |   1001001|     중구|35.87343028|128.6103158|
        |   1001002|     중구|35.87334197|128.6099071|
        |   1001003|     중구|35.87327842|128.6096135|
        +----------+--------+-----------+-----------+

        >>> df = CoordDataFrame(origin_sdf, gdf, tdf, 'lon', 'lat')
        >>> res_df = df.coord_to_jibun()
        >>> res_df.show()
        +----------+--------+-----------+-----------+----------+-------+------------+---------+--------------------+----------------------+
        |	가로등번호|	 관할구청|        위도|        경도|      sido|sigungu|eupmyeondong|bupjungli|jibun_primary_number|jibun_secondary_number|
        +----------+--------+-----------+-----------+----------+-------+------------+---------+--------------------+----------------------+
        |   1001001|     중구|35.87343028|128.6103158|   대구광역시|    중구|     동인동3가|         |                 192|                    79|
        |   1001002|     중구|35.87334197|128.6099071|   대구광역시|    중구|     동인동3가|         |                 192|                    79|
        |   1001003|     중구|35.87327842|128.6096135|   대구광역시|    중구|     동인동3가|         |                 192|                    79|
        +----------+--------+-----------+-----------+----------+-------+------------+---------+--------------------+----------------------+
        """
        joined_df = self.joined_df.select(
            "PNU",
            "sido",
            "sigungu",
            "eupmyeondong",
            "bupjungli",
            "jibun_primary_number",
            "jibun_secondary_number",
        )
        res_df = self.pnu_df.join(joined_df, "PNU", "leftouter").drop("PNU")
        res_df = res_df.dropDuplicates([self._x_colname, self._y_colname])
        return res_df

    def join_with_db(self):
        """
        Summary
        -------
        위경도 좌표가 포함된 원본 Spark DataFrame에 데이터베이스에서 가져온 Spark DataFrame 정보를 추가합니다.

        Usage
        -------
        >>> from sparkplus.core.sparkplus import CoordDataFrame
        >>> df = CoordDataFrame(origin_sdf, gdf, tdf, 'lon', 'lat')
        >>> res_df = df.join_with_table()

        Example
        -------
        >>> origin_sdf.show()
        +----------+--------+-----------+-----------+
        |   가로등번호|  관할구청|        위도|        경도|
        +----------+--------+-----------+-----------+
        |   1001001|     중구|35.87343028|128.6103158|
        |   1001002|     중구|35.87334197|128.6099071|
        |   1001003|     중구|35.87327842|128.6096135|
        +----------+--------+-----------+-----------+

        >>> df = CoordDataFrame(origin_sdf, gdf, tdf, '경도', '위도')
        >>> res_df = df.join_with_table()
        >>> res_df.show()
        +----------+--------+-----------+-----------+-------------------+--------------------+-------------+-------+----------+-------+------------+---------+---------------+-----------+-----------------------+-------------------------+--------------------+----------------------+----------------+
        |	가로등번호|	 관할구청|        위도|        경도|                PNU|       manage_number|roadname_code|zipcode|      sido|sigungu|eupmyeondong|bupjungli|       roadname|is_basement|building_primary_number|building_secondary_number|jibun_primary_number|jibun_secondary_number|bupjungdong_code|
        +----------+--------+-----------+-----------+-------------------+--------------------+-------------+-------+----------+-------+------------+---------+---------------+-----------+-----------------------+-------------------------+--------------------+----------------------+----------------+
        |   1065002|     중구|35.86341579|128.6024286|2711010600101990000|27110106001000300...| 271103007017|  41940|	대구광역시|    중구|   	 삼덕동2가|         |           공평로|          0|                     46|                        0|                   3|                     4|      2711010600|
        |   1063002|     중구|35.86516734|128.6105401|2711010700103790000|27110107001003100...| 271104223055|  41945| 	대구광역시|    중구|   	 삼덕동3가|         |	 달구벌대로443길|          0|                     62|                       16|                  31|                     2|      2711010700|
        |   1024017|     중구|35.86927185|128.5937782|2711011700101200003|27110115001008500...| 271102007001|  41909|	대구광역시|    중구|        남일동|         |         중앙대로|          1|                    424|                        0|                 143|                     1|      2711011700|
        +----------+--------+-----------+-----------+-------------------+--------------------+-------------+-------+----------+-------+------------+---------+---------------+-----------+-----------------------+-------------------------+--------------------+----------------------+----------------+

        """
        return self.joined_df
