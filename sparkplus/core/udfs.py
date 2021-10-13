from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType, ArrayType

sido_short_list = ["서울", "부산", "대구", "인천", "광주", "대전", "울산", "세종", "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주"]
sido_long_list = ["서울특별시","부산광역시","대구광역시","인천광역시","광주광역시","대전광역시","울산광역시","세종특별자치시",'경기도',"강원도","충청북도","충청남도","전라북도","전라남도","경상북도","경상남도","제주특별자치도"]
sido_dictionary = dict(zip(sido_short_list, sido_long_list))
sido_reverse_dictionary = dict(zip(sido_short_list, sido_long_list))

@udf(IntegerType())
def where_is_sido(split):
	for i in range(len(split)):
		if sido_dictionary.get(split[i]) or sido_reverse_dictionary.get(split[i]):
			return i
	return -1

@udf(ArrayType(StringType()))
def cleanse_split(idx, split):
	if idx != -1:
		return split[idx:]
	return split

@udf(StringType())
def extract_sido(split):
	for data in split:
		if sido_dictionary.get(data):
			return sido_dictionary[data]
		elif sido_reverse_dictionary.get(data):
			return data
	return "None"

@udf(StringType())
def extract_sigungu(split):
    for data in split:
        if not sido_reverse_dictionary.get(data):
            sigungu = data[-1]
            if (sigungu == '시') or (sigungu == '군') or (sigungu == '구'):
                return data
    return "None"