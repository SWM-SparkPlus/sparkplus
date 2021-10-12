from typing import Type
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, split
from pyspark.sql.types import IntegerType, StringType, ArrayType

class RoadnameDataframe(object):
	"""
	도로명 주소를 활용하여 데이터를 분석하기 위한 클래스입니다
	"""
	def __init__(self, dataFrame: DataFrame):
		self._df = dataFrame
		self._sido_short_list = ["서울", "부산", "대구", "인천", "광주", "대전", "울산", "세종", "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주"]
		self._sido_long_list = ["서울특별시","부산광역시","대구광역시","인천광역시","광주광역시","대전광역시","울산광역시","세종특별자치시",'경기도',"강원도","충청북도","충청남도","전라북도","전라남도","경상북도","경상남도","제주특별자치도"]
		self._sido_dictionary = dict(zip(self._sido_short_list, self._sido_long_list))
		self._sido_reverse_dictionary = dict(zip(self._sido_long_list, self._sido_short_list))

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
		@udf(IntegerType())
		def where_is_sido(split):
			for i in range(len(split)):
				if self._sido_dictionary.get(split[i]) or self._sido_reverse_dictionary.get(split[i]):
					return i
			return -1

		@udf(ArrayType(StringType()))
		def cleanse_split(idx, split):
			if idx != -1:
				return split[idx:]
			return split

		self._df = self._df \
						.withColumn('idx', where_is_sido(self._df.split)) \
						.withColumn('split', cleanse_split(self.idx, self.split))
		self._df = self._df.drop('idx')
		return RoadnameDataframe(self._df)
