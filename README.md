# SparkPlus
Spark+는 H3, 위/경도 좌표 등의 공간 정보를 국내 주소체계(신주소/구주소)와 함께 처리할 수 있도록 지원하는 Package입니다.

## Spark+ 전체 아키텍처

![](https://github.com/SWM-SparkPlus/sparkplus/blob/master/statics/sparkplus_arch_finale.png)

## Setup

[개발자 가이드 참고](https://github.com/SWM-SparkPlus/sparkplus/wiki)

- Spark+는 PyPI에 배포되어 있으며, 다음 커맨드로 설치할 수 있습니다.
```
$ pip install sparkplus
```

- 설치 후에 import하여 사용할 수 있습니다.
```
from sparkplus.core import CoordDataFrame, AddressDataFrame
```

## Class

### CoordDataFrame
위치 좌표를 포함하는 데이터프레임을 주소체계 데이터베이스와 연동하여 pnu코드, h3, 우편번호, 법정동코드, 도로명주소(시도/시군구/읍면동/법정리/도로명/지하여부/건물 본번/건물 부번), 도로명주소(전체), 지번주소(시도/시군구/읍면동/법정리/지번 본번/지번 분번) 등의 컬럼을 추가합니다.
```
coord_df = CoordDataFrame(source_df, geo_df, table_df, x_colname, y_colname)
```
|        위도|        경도|                PNU|       manage_number|roadname_code|zipcode|      sido|sigungu|eupmyeondong|bupjungli|       roadname|is_basement|building_primary_number|building_secondary_number|jibun_primary_number|jibun_secondary_number|bupjungdong_code|
|-----------|-----------|-------------------|--------------------|-------------|-------|----------|-------|------------|---------|---------------|-----------|-----------------------|-------------------------|--------------------|----------------------|----------------|
|35.86341579|128.6024286|2711010600101990000|27110106001000300...| 271103007017|  41940|	대구광역시|    중구|   	 삼덕동2가|         |           공평로|          0|                     46|                        0|                   3|                     4|      2711010600|
|35.86516734|128.6105401|2711010700103790000|27110107001003100...| 271104223055|  41945| 	대구광역시|    중구|   	 삼덕동3가|         |	 달구벌대로443길|          0|                     62|                       16|                  31|                     2|      2711010700|
|35.86927185|128.5937782|2711011700101200003|27110115001008500...| 271102007001|  41909|	대구광역시|    중구|        남일동|         |         중앙대로|          1|                    424|                        0|                 143|                     1|      2711011700|
 
### AddressDataFrame
비정형 도로명주소 또는 지번주소를 포함하는 데이터프레임을 주소체계 데이터베이스와 연동하여 분석 및 시각화할 수 있는 형태의 시도, 시군구, 읍면동,  법정동코드, 시군구코드 등의 컬럼을 추가합니다.
```
roadname_df = AddressDataFrame(source_df).to_bupjungdong("target_colname", table_df)
```

|받는분주소| sido_name|sigungu_name|eupmyeondong_name|bupjungdong_code|sigungu_code|
|-------|----------|------------|-----------------|----------------|------------|
|서울특별시 강남구 가로수길 75|서울특별시|      강남구|           신사동|      1168010700|       11680|
|서울특별시 강남구 강남대로 346|서울특별시|      강남구|           역삼동|      1168010100|       11680|
|서울특별시 강남구 논현로 120길 20|서울특별시|      강남구|           논현동|      1168010800|       11680|

## LICENSE
[MIT](https://github.com/SWM-SparkPlus/db-updater/blob/master/LICENSE)
