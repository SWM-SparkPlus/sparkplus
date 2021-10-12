from typing import Type
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, split
from pyspark.sql.types import StringType

class RoadnameDataframe(object):
	"""
	도로명 주소를 활용하여 데이터를 분석하기 위한 클래스 입니다.
	"""
	def __init__(self, dataFrame: DataFrame):
		self._df = dataFrame
		self._sido_short_list = ["서울", "부산", "대구", "인천", "광주", "대전", "울산", "세종", "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주"]
		self._sido_long_list = ["서울특별시","부산광역시","대구광역시","인천광역시","광주광역시","대전광역시","울산광역시","세종특별자치시",'경기도',"강원도","충청북도","충청남도","전라북도","전라남도","경상북도","경상남도","제주특별자치도"]
		self._sido_dictionary = dict(zip(self.sido_short_list, self.sido_long_list))
		self._sido_reverse_dictionary = dict(zip(self.sido_long_list, self.sido_short_list))

	def add_split_column(self, target: str):
		"""
		DB에서 조회를 위해 원본의 string을 공백 기준으로 나누는 함수 입니다

		Parameters
		----------
		target : str
			split하고 조작할 원본 데이터의 컬럼명 입니다. 
		
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
		df = self._df.withColumn('split', split(self._df[target], ' '))
		return df