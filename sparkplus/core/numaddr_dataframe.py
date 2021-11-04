import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))))

from pyspark.sql import DataFrame
from pyspark.sql.functions import split, col
from sparkplus.core.udfs import *

class NumAddrDataFrame(object):
    """
    도로명 주소를 활용하여 데이터를 분석하기 위한 클래스입니다
    """
    def __init__(self, dataFrame: DataFrame):
        self._df = dataFrame
        self._tmp_df = dataFrame
        self.col_list = dataFrame.columns

    def to_bupjungdong(self, target: str, db_df:DataFrame):
        """
        도로명을 지번으로 변경하는 전 과정을 포함하는 함수입니다
        """
        self.add_split(target)
        self.add_sido()
        self.add_sigungu()
        self.add_eupmyeondong()
        self.add_jibun_primary()
        self.add_jibun_secondary()
        self.join_with_db(db_df)
        # self.join_with_db(db_df)
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
        self._df = self._df.withColumn('split', split(self._df[target], ' '))
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

        self._df = self._df \
                        .withColumn('idx', where_is_sido(self._df.split)) \
                        .withColumn('split', cleanse_split(self._df.idx, self._df.split))
        self._df = self._df.drop('idx')
        self._df = self._df.withColumn('split', process_numaddr(self._df.split))
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
        self._df.show()
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
        self._df.show()
        return self._df

    def add_eupmyeondong(self):
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
        self._df.show()
        return self._df

    def add_jibun_primary(self):
        self._df = self._df.withColumn("jibun_primary_number", extract_jibun_primary(self._df.split))
        self._df.show()
        return self._df

    def add_jibun_secondary(self):
        self._df = self._df.withColumn("jibun_secondary_number", extract_jibun_secondary(self._df.split))
        self._df.show()
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
        tmp_db_df = db_df.select( \
                    col("sido").alias("db_sido"), \
                    col("sigungu").alias("db_sigungu"), \
                    col("eupmyeondong").alias("db_eupmyeondong"), \
                    col("roadname").alias("db_roadname"), \
                    col("jibun_primary_number").alias("db_jibun_primary_number"), \
                    col("jibun_secondary_number").alias("db_jibun_secondary_number"), \
                    col("bupjungdong_code").alias("db_bupjungdong_code") \
                    ) \
                    #.drop_duplicates(['db_roadname', 'db_building_primary_number'])
        tmp_df = self._df.join(tmp_db_df, (self._df.sigungu == tmp_db_df.db_sigungu) & (self._df.eupmyeondong == tmp_db_df.db_eupmyeondong) & (self._df.jibun_primary_number == tmp_db_df.db_jibun_primary_number) & (self._df.jibun_secondary_number == tmp_db_df.db_jibun_secondary_number), 'inner')
        tmp_df = tmp_df.withColumnRenamed("db_bupjungdong_code", "bupjungdong_code")
        self._df = tmp_df.select(self._df['*'], "bupjungdong_code")
        del self._tmp_df
        del tmp_df

        return self._df

        
