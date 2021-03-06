import sys
import os

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
)

from pyspark.sql import DataFrame
from pyspark.sql.functions import split, col, lit
from sparkplus.core.udfs import *
from pyspark.sql.functions import when


class AddressDataFrame(object):
    """
    도로명 주소를 활용하여 데이터를 분석하기 위한 클래스입니다
    """

    def __init__(self, dataFrame: DataFrame):
        self._df = dataFrame
        self._tmp_df = dataFrame
        self.col_list = dataFrame.columns

    def to_bupjungdong(self, target: str, db_df: DataFrame):
        """
        도로명을 지번으로 변경하는 전 과정을 포함하는 함수입니다
        """
        self.add_split(target)
        self.add_sido()
        self.add_sigungu()
        self.add_eupmyeon()
        self.add_dong()
        self.add_roadname()
        self.add_building_primary_number()
        self.add_jibun_primary_number()
        self.join_with_db(db_df)
        return self._df

    def add_split(self, target: str):
        """
        DB에서 조회를 위해 원본의 string을 공백 기준으로 나누는 함수입니다.

        Parameters
        ----------
        target : str
            split하고 조작할 원본 데이터의 컬럼명

        Examples
        --------
        >>> road_df = RoadnameDataframe(your_df)
        >>> road_df._df.show()
        +------------------------------+s
        |target                        |
        +------------------------------+
        |경기도 화성시 장안면 매바위로366번길 8 |
        |경기도 화성시 장안면 버들로          |
        |경기도 화성시 장안면 석포리          |
        +------------------------------+

        >>> splited_df = road_df.add_split('target')
        >>> splited_df.show()
        +------------------------------+-----------------------------------+
        |target                        |split                              |
        +------------------------------+-----------------------------------+
        |경기도 화성시 장안면 매바위로366번길 8|[경기도, 화성시, 장안면, 매바위로366번길, 8]|
        |경기도 화성시 장안면 버들로         |[경기도, 화성시, 장안면, 버들로]           |
        |경기도 화성시 장안면 석포리         |[경기도, 화성시, 장안면, 석포리]           |
        +-----------------------------+------------------------------------+
        """
        self._df = self._df.withColumn("split", split(self._df[target], " "))
        return self._df

    def cleanse_split_column(self):
        """
        주소가 비정형 데이터일 경우 사용되는 함수입니다.
        add_split_column 함수로 쪼개진 split 컬럼의 데이터를 전처리합니다.

        UDF
        ---
        where_is_sido : IntegerType
            split 컬럼에서 특별시와 광역시, 도를 찾고, 위치한 인덱스를 반환합니다.

            Exmaple
            -------
            >>> df.show()
            +---------------------------------------------+
            |split                                        |
            +---------------------------------------------+
            |[[185-74], 경기도, 화성시, 장안면,매바위로366번길, 8]|
            |[경기도, 화성시, 장안면, 버들로]                   |
            |[경기도, 화성시, 장안면, 석포리]                   |
            +--------------------------------------------+

            >>> df.withColumn('idx', where_is_sido(split)).show()
            +---------------------------------------------+----+
            |split                                        |sido|
            +---------------------------------------------+----+
            |[[185-74], 경기도, 화성시, 장안면,매바위로366번길, 8]|   1|
            |[경기도, 화성시, 장안면, 버들로]                   |   0|
            |[경기도, 화성시, 장안면, 석포리]                   |   2|
            +--------------------------------------------+----+

        cleanse_split: ArrayType(StringType)
            split 컬럼과 인덱스 컬럼을 활용하여 알맞은 주소체계 값으로 반환합니다.

            Example
            -------
            >>> df.show()
            +------------------------------------------------+---+
            |split                                           |idx|
            +------------------------------------------------+---+
            |[[185-74], 경기도, 화성시, 장안면,매바위로366번길, 8]    | 1|
            |[경기도, 화성시, 장안면, 버들로]                       | 0|
            |[Gyeonggi-do, [185-74], 경기도, 화성시, 장안면, 석포리]| 2|
            +------------------------------------------------+---+

            >>> df.withColumn('split', cleanse_split(df.split))
            +----------------------------------------+
            |split                                   |
            +----------------------------------------+
            |[경기도, 화성시, 장안면,매바위로366번길, 8]     |
            |[경기도, 화성시, 장안면, 버들로]              |
            |[경기도, 화성시, 장안면, 석포리]              |
            +---------------------------------------+
        """

        self._df = self._df.withColumn("idx", where_is_sido(self._df.split)).withColumn(
            "split", cleanse_split(self._df.idx, self._df.split)
        )
        self._df = self._df.drop("idx")
        self._df = self._df.withColumn("split", process_roadname(self._df.split))
        return self._df

    def add_sido(self):
        """
        특별시, 광역시, 도를 기존 데이터프레임에 추가하는 함수입니다.

        UDF
        ---
        extract_sido : StringType
            split 컬럼에서 특별시와 광역시, 도를 찾고 값을 반환합니다.
            값이 없는 경우, "None" : str 을 반환합니다.

            Exmaple
            -------
            >>> df.show()
            +----------------------------------------+
            |split                                   |
            +----------------------------------------+
            |[경기도, 안산시, 단원구, 해봉로, 137]          |
            |[경기도, 수원시, 장안구, 경수대로, 1079]        |
            |[경기도, 안산시, 상록구, 양달말길, 93-7]        |
            +----------------------------------------+

            >>> df.withColumn('idx', extract_sido()).show()
            +----------------------------------------------+-----+
            |split                                         |sido |
            +----------------------------------------------+-----+
            |[경기도, 안산시, 단원구, 해봉로, 137]                |경기도 |
            |[경기도, 수원시, 장안구, 경수대로, 1079]              |경기도 |
            |[경기도, 안산시, 상록구, 양달말길, 93-7]              |경기도 |
            +----------------------------------------------+------+
        """

        self._df = self._df.withColumn("sido", extract_sido(self._df.split))
        return self._df

    def add_sigungu(self):
        """
        시, 군, 구 컬럼을 기존 데이터프레임에 추가하는 함수입니다.
        UDF
        ---
        extract_sigungu : StringType
            split 컬럼에서 시, 군, 구를 찾고 값을 반환합니다.

            시와 구가 같이 있을경우에는 시와 구를 같이 반환합니다.
            ex) 경기도 성남시 분당구 -> 성남시 분당구

            값이 없는 경우, "None" : str 을 반환합니다.

            Exmaple
            -------
            >>> df.show()
            +----------------------------------------------+-----+
            |split                                         |sido |
            +----------------------------------------------+-----+
            |[경기도, 안산시, 단원구, 해봉로, 137]                |경기도 |
            |[경기도, 수원시, 장안구, 경수대로, 1079]              |경기도 |
            |[경기도, 안산시, 상록구, 양달말길, 93-7]              |경기도 |
            +----------------------------------------------+------+

            >>> df.withColumn('idx', extract_sigungu()).show()
            +----------------------------------------------+------+-----------+
            |split                                         |sido  |sigungu    |
            +----------------------------------------------+------+-----------+
            |[경기도, 안산시, 단원구, 해봉로, 137]                |경기도 |안산시 단원구 |
            |[경기도, 수원시, 장안구, 경수대로, 1079]             |경기도  |수원시 장안구 |
            |[경기도, 안산시, 상록구, 양달말길, 93-7]             |경기도  |안산시 상록구 |
            +----------------------------------------------+------+-----------+
        """

        self._df = self._df.withColumn("sigungu", extract_sigungu(self._df.split))
        return self._df

    def add_eupmyeon(self):
        """
        읍, 면 컬럼을 기존에 데이터프레임에 추가하는 함수입니다.

        UDF
        ---
        extract_eupmyeon : StringType
            split 컬럼에서 읍이나 면을 찾고 값을 반환합니다.

            값이 없는 경우, "None" : str 을 반환합니다.

            Exmaple
            -------
            >>> df.show()
            +----------------------------------------------+------+-----------+
            |split                                         |sido  |sigungu    |
            +----------------------------------------------+------+-----------+
            |[경기도, 화성시, 장안면, 매바위로366번길, 8]           |경기도 |화성시       |
            |[강원도, 원주시, 호저면, 사제로, 9]                  |강원도  |원주시      |
            |[경상남도, 사천시, 곤양면, 경충로, 23-1]              |경상남도|사천시      |
            +----------------------------------------------+------+-----------+

            >>> df.withColumn('idx', extract_eupmyeon()).show()
            +----------------------------------------------+------+-----------+--------+
            |split                                         |sido  |sigungu    |eupmyeon|
            +----------------------------------------------+------+-----------+--------+
            |[경기도, 화성시, 장안면, 매바위로366번길, 8]           |경기도 |화성시       |장안면   |
            |[강원도, 원주시, 호저면, 사제로, 9]                  |강원도  |원주시      |호저면   |
            |[경상남도, 사천시, 곤양면, 경충로, 23-1]              |경상남도|사천시      |곤양면   |
            +----------------------------------------------+------+-----------+-------+
        """
        self._df = self._df.withColumn("eupmyeondong", extract_eupmyeondong(self._df.split))
        return self._df

    def add_dong(self):
        """
        데이터프레임에 동이 포함되어있는지 확인하고 동 컬럼을 추가하는 함수입니다.

        UDF
        ---
        extract_dong : StringType
            split 컬럼에서 읍이나 면을 찾고 값을 반환합니다.

            값이 없는 경우, "None" : str 을 반환합니다.

            Exmaple
            -------
            >>> df.show()
            +-------------------------+--------+-----------+
            |split                    |sido    |sigungu    |
            +-------------------------+--------+-----------+
            |[경기도, 성남시, 분당구, 금곡동]|경기도   |성남시       |
            |[충청남도, 공주시, 검상동]     |강원도   |공주시       |
            |[대전광역시, 동구, 가오동]     |대전광역시|동구         |
            +-------------------------+--------+-----------+

            >>> df.withColumn('idx', extract_dong()).show()
            +-------------------------+--------+-----------+----+
            |split                    |sido    |sigungu    |dong|
            +-------------------------+--------+-----------+----+
            |[경기도, 성남시, 분당구, 금곡동]|경기도   |성남시       |금곡동|
            |[충청남도, 공주시, 검상동]     |강원도   |공주시       |검상동|
            |[대전광역시, 동구, 가오동]     |대전광역시|동구         |가오동|
            +-------------------------+--------+-----------+-----+
        """

        self._df = self._df.withColumn("dong", extract_dong(self._df.split))
        return self._df

    def add_roadname(self):
        """
        데이터프레임에 도로명주소 컬럼을 추가하는 함수입니다.
        UDF
        ---
        extract_building_primary_number : StringType
            split 컬럼에서 도로명를 찾고 값을 반환합니다.

            값이 없는 경우, "None" : str 을 반환합니다.

            Exmaple
            -------
            >>> df.show()
            +----------------------------------------------+------+-----------+
            |split                                         |sido  |sigungu    |
            +----------------------------------------------+------+-----------+
            |[경기도, 안산시, 단원구, 해봉로, 137]                |경기도 |안산시 단원구 |
            |[경기도, 수원시, 장안구, 경수대로, 1079]             |경기도  |수원시 장안구 |
            |[경기도, 안산시, 상록구, 양달말길, 93-7]             |경기도  |안산시 상록구 |
            +----------------------------------------------+------+-----------+

            >>> df.withColumn('idx', add_sigungu()).show()
            +----------------------------------------------+------+-----------+---------+
            |split                                         |sido  |sigungu    |roadname |
            +----------------------------------------------+------+-----------+---------+
            |[경기도, 안산시, 단원구, 해봉로, 137]                |경기도 |안산시 단원구 |해봉로     |
            |[경기도, 수원시, 장안구, 경수대로, 1079]             |경기도  |수원시 장안구 |경수대로   |
            |[경기도, 안산시, 상록구, 양달말길, 93-7]             |경기도  |안산시 상록구 |양달말길   |
            +----------------------------------------------+------+-----------+---------+
        """
        self._df = self._df.withColumn("roadname", extract_roadname(self._df.split))
        return self._df

    def add_building_primary_number(self):
        """
        데이터프레임에 도로명주소의 건물본번을 추가하는 함수입니다.

        UDF
        ---
        extract_building_primary_number : StringType

            Parameters
            ----------
            split : columnType
            roadname : columnType

            roadname 뒤에 건물 본번과 부번이 들어오면 건물 본번을 반환합니다..

            값이 없는 경우, "None" : str 을 반환합니다.

            Exmaple
            -------
            >>> df.show()
            +----------------------------------------------+------+-----------+---------+
            |split                                         |sido  |sigungu    |roadname |
            +----------------------------------------------+------+-----------+---------+
            |[경기도, 안산시, 단원구, 해봉로, 137]                |경기도 |안산시 단원구 |해봉로     |
            |[경기도, 수원시, 장안구, 경수대로, 1079]             |경기도  |수원시 장안구 |경수대로   |
            |[경기도, 안산시, 상록구, 양달말길, 93-7]             |경기도  |안산시 상록구 |양달말길   |
            +----------------------------------------------+------+-----------+---------+

            >>> df.withColumn('idx', extract_building_primary_number()).show()
            +----------------------------------------------+------+-----------+---------+-----------------------+
            |split                                         |sido  |sigungu    |roadname |building_primary_number|
            +----------------------------------------------+------+-----------+---------+-----------------------+
            |[경기도, 안산시, 단원구, 해봉로, 137]                |경기도 |안산시 단원구 |해봉로      |137                    |
            |[경기도, 수원시, 장안구, 경수대로, 1079]             |경기도  |수원시 장안구 |경수대로    |1079                   |
            |[경기도, 안산시, 상록구, 양달말길, 93-7]             |경기도  |안산시 상록구 |양달말길    |93                     |
            +----------------------------------------------+------+-----------+---------+-----------------------+
        """
        self._df = self._df.withColumn(
            "building_primary_number",
            extract_building_primary_number(self._df.split, self._df.roadname),
        )
        return self._df
    
    def add_jibun_primary_number(self):
        self._df = self._df.withColumn(
            "jibun_primary_number",
            extract_jibun_primary_number(self._df.split, self._df.roadname),
        )
        return self._df

    def join_with_db(self, db_df):

        """
        데이터베이스 데이터프레임과 조인하는 함수입니다.

        Parameters
        ----------
        db_df : DataFrame


        Exmaple
        -------
        >>> df.show()
        +----------------------------------------------+------+-----------+---------+-----------------------+
        |split                                         |sido  |sigungu    |roadname |building_primary_number|
        +----------------------------------------------+------+-----------+---------+-----------------------+
        |[경기도, 안산시, 단원구, 해봉로, 137]                |경기도 |안산시 단원구 |해봉로      |137                    |
        |[경기도, 수원시, 장안구, 경수대로, 1079]             |경기도  |수원시 장안구 |경수대로    |1079                   |
        |[경기도, 안산시, 상록구, 양달말길, 93-7]             |경기도  |안산시 상록구 |양달말길    |93                     |
        +----------------------------------------------+------+-----------+---------+-----------------------+

        >>> df.withColumn('idx', extract_building_primary_number()).show()
        +----------------------------------------------+------+-----------+---------+-----------------------+----------------+
        |split                                         |sido  |sigungu    |roadname |building_primary_number|bupjungdong_code|
        +----------------------------------------------+------+-----------+---------+-----------------------+----------------+
        |[경기도, 안산시, 단원구, 해봉로, 137]                |경기도 |안산시 단원구 |해봉로      |137                    |4128112400     |
        |[경기도, 수원시, 장안구, 경수대로, 1079]             |경기도  |수원시 장안구 |경수대로    |1079                   |4128111800     |
        |[경기도, 안산시, 상록구, 양달말길, 93-7]             |경기도  |안산시 상록구 |양달말길    |93                     |4128101100     |
        +----------------------------------------------+------+-----------+---------+-----------------------+---------------+
        """
        db_df_roadname = db_df.select(
            col("sido").alias("sido_name"),
            col("sigungu").alias("sigungu_name"),
            col("eupmyeondong").alias('eupmyeondong_name'),
            col("roadname").alias("db_roadname"),
            col("building_primary_number").alias("db_building_primary_number"),
            col("bupjungdong_code").alias('db_bupjungdong_code'),
            col("jibun_primary_number").alias("db_jibun_primary_number")
        ).drop_duplicates(["sigungu_name", "db_roadname", "db_building_primary_number"])

        db_df_jibun = db_df.select(
            col("sido").alias("sido_name"),
            col("sigungu").alias("sigungu_name"),
            col("eupmyeondong").alias('eupmyeondong_name'),
            col("roadname").alias("db_roadname"),
            col("building_primary_number").alias("db_building_primary_number"),
            col("bupjungdong_code").alias('db_bupjungdong_code'),
            col("jibun_primary_number").alias("db_jibun_primary_number")
        ).drop_duplicates(["sigungu_name", "eupmyeondong_name", "db_jibun_primary_number"])

        jibun_origin = self._df.where(self._df.roadname == "None")
        roadname_origin = self._df.where(self._df.roadname != "None")

        join_df_roadname = roadname_origin.join(
            db_df_roadname,  
            (self._df.sigungu == db_df_roadname.sigungu_name)
            & (self._df.roadname == db_df_roadname.db_roadname)
            & (self._df.building_primary_number == db_df_roadname.db_building_primary_number),
            "inner",
        ) \
         .withColumnRenamed("db_bupjungdong_code", "bupjungdong_code") \
         .select(*self.col_list, "sido_name", "sigungu_name", "eupmyeondong_name", "bupjungdong_code")

        
        join_df_jibun = jibun_origin.join(
            db_df_jibun,
            (self._df.sigungu == db_df_jibun.sigungu_name)
            & (self._df.eupmyeondong == db_df_jibun.eupmyeondong_name)
            & (self._df.jibun_primary_number == db_df_jibun.db_jibun_primary_number),
            "inner",
        ) \
        .withColumnRenamed("db_bupjungdong_code", "bupjungdong_code") \
        .select(*self.col_list, "sido_name", "sigungu_name", "eupmyeondong_name", "bupjungdong_code")
      
        self._df = join_df_roadname.union(join_df_jibun)

        self._df = self._df.withColumn("sigungu_code", extract_sigungu_code(self._df.bupjungdong_code))

        return self._df
