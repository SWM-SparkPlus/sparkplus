from typing import Type
from pyspark.sql import DataFrame
from pyspark.sql.functions import split
from udfs import extract_sigungu, extract_dong, where_is_sido, cleanse_split, extract_sido

class RoadnameDataframe(object):
    """
    도로명 주소를 활용하여 데이터를 분석하기 위한 클래스입니다
    """
    def __init__(self, dataFrame: DataFrame):
        self._df = dataFrame

    def roadname_to_bupjeongdong_code(self, target: str):
        """
        도로명을 지번으로 변경하는 전 과정을 포함하는 함수입니다
        """
        self.add_split_column(target)
        self.cleanse_split_column()
        return RoadnameDataframe(self._df)

    def add_split_column(self, target: str):
        """
        DB에서 조회를 위해 원본의 string을 공백 기준으로 나누는 함수

        Parameters
        ----------
        target : str
            split하고 조작할 원본 데이터의 컬럼명

        Examples
        --------
        >>> road_df = RoadnameDataframe(your_df)
        >>> road_df._df.show()
        +-----------------------------------------------------------------------+
        |target                                                                 |
        +-----------------------------------------------------------------------+
        |경기도 화성시 장안면 매바위로366번길 8											|
        |경기도 화성시 장안면 버들로                                           	 	  |
        |경기도 화성시 장안면 석포리                                                  |
        +-----------------------------------------------------------------------+

        >>> splited_df = road_df.add_split_column('target')
        >>> splited_df.show()
        +-----------------------------------------------------------------------+----------------------------------------------------------------------------------+
        |target                                                                 |split                                                                             |
        +-----------------------------------------------------------------------+----------------------------------------------------------------------------------+
        |경기도 화성시 장안면 매바위로366번길 8					               		   	 |[경기도, 화성시, 장안면, 매바위로366번길, 8]					                   	     	|
        |경기도 화성시 장안면 버들로                                    			   |[경기도, 화성시, 장안면, 버들로]                                 		                 |
        |경기도 화성시 장안면 석포리                                        	       |[경기도, 화성시, 장안면, 석포리]                                         		         |
        +-----------------------------------------------------------------------+----------------------------------------------------------------------------------+
        """
        if hasattr(self._df, target):
            raise TypeError("Dataframe does not have" + str + "column")
        self._df = self._df.withColumn('split', split(self._df[target], ' '))
        return RoadnameDataframe(self._df)

    def cleanse_split_column(self):
        """
        add_split_column 함수로 쪼개진 split 컬럼의 데이터를 전처리합니다

        UDF
        ---
        where_is_sido : IntegerType
            split 컬럼에서 특별시와 광역시, 도를 찾고, 위치한 인덱스를 반환합니다

            Exmaple
            -------
            >>> df.show()
            +------------------------------+
            |                         split|
            +------------------------------+
            |      [[185-74], 경기도, 화...  |
            |    [185-74, 경기도, 화성시...	  |
            |       [[445-941], 경기, 화...  |
            |          [Gyeonggi-do, Hwa...|
            +------------------------------+

            >>> df.withColumn('idx', where_is_sido(split)).show()
            +------------------------------+---+
            |                         split|idx|
            +------------------------------+---+
            |      [[185-74], 경기도, 화...  |  1|
            |    [185-74, 경기도, 화성시...   |  1|
            |       [[445-941], 경기, 화...  |  1|
            |          [Gyeonggi-do, Hwa...|  5|
            +------------------------------+---+

        cleanse_split: ArrayType(StringType)
            split 컬럼에서 찾은 도와 특별, 광역시의 인덱스부터 끝까지 값을 반환합니다

            Example
            -------
            >>> df.show()
            +-------------------------------+
            |                          split|
            +-------------------------------+
            |		  [[185-74], 경기도, 화...|
            |   	 [185-74, 경기도, 화성시...|
            |      	  [[445-941], 경기, 화...|
            +-------------------------------+

            >>> df.withColumn('split', cleanse_split(df.split))
            +--------------------------------+
            |                           split|
            +--------------------------------+
            |  	   [경기도, 화성시, 장안면, 매...  |
            |		 [경기도, 화성시, 장안면, 돌...|
            | 	   [경기, 화성시, 장안면, 석포...  |
            +--------------------------------+
        """

        self._df = self._df \
                        .withColumn('idx', where_is_sido(self._df.split)) \
                        .withColumn('split', cleanse_split(self._df.idx, self._df.split))
        self._df = self._df.drop('idx')
        return RoadnameDataframe(self._df)

    def add_sido(self):
        """
        특별시, 광역시, 도를 기존 데이터프레임에 추가하는 함수입니다.

        UDF
        ---
        extract_sido : StringType
            split 컬럼에서 특별시와 광역시, 도를 찾고 값을 반환합니다.
            값이 없는 경우, "None" : str 을 반환합니다.
        """

        self._df = self._df.withColumn("sido", extract_sido(self._df.split))
        return RoadnameDataframe(self._df)

    def add_sigungu(self):
        """
        시, 군, 구 컬럼을 기존 데이터프레임에 추가하는 함수입니다.
        """

        self._df = self._df.withColumn("sigungu", extract_sigungu(self._df.split))
        return RoadnameDataframe(self._df)

    def add_dong(self):
        """
        데이터프레임에 동이 포함되어있는지 확인하고 동 컬럼을 추가하는 함수입니다.
        """

        self._df = self._df.withColumn("dong", extract_dong(self._df.split))
        return RoadnameDataframe(self._df)
    