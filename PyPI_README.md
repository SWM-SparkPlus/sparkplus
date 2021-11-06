# SparkPlus
Spark+는 H3, 위/경도 좌표 등의 공간 정보를 국내 주소체계(신주소/구주소)와 함께 처리할 수 있도록 지원하는 Package입니다.

## Setup

[GitHub](https://github.com/SWM-SparkPlus/sparkplus/)
[개발자 가이드 참고](https://github.com/SWM-SparkPlus/sparkplus/wiki)

- Spark+는 PyPI에 배포되어 있으며, 다음 커맨드로 설치할 수 있습니다.
```
$ pip install sparkplus
```

- 설치 후에 import하여 사용할 수 있습니다.
```
from sparkplus.core import CoordDataFrame, RoadnameDataFrame, NumAddrDataFrame
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
 
### RoadnameDataFrame
비정형 도로명주소를 포함하는 데이터프레임을 주소체계 데이터베이스와 연동하여 분석 및 시각화할 수 있는 형태로 전처리한 시도, 시군구, 읍면동, 도로명, 건물 본번, 법정동코드 등의 컬럼을 추가합니다.
```
roadname_df = RoadnameDataFrame(source_df)
```
 |target                                  |sido  |sigungu    |roadname |building_primary_number|bupjungdong_code|
 |----------------------------------------|------|-----------|---------|-----------------------|----------------|
 |경기도 안산시 단원구 해봉로 137                |경기도 |안산시 단원구 |해봉로      |137                    |4128112400     |
 |경기도 수원시 장안구 경수대로 1079             |경기도  |수원시 장안구 |경수대로    |1079                   |4128111800     |
 |경기도 안산시 상록구 양달말길 93-7             |경기도  |안산시 상록구 |양달말길    |93                     |4128101100     |


## LICENSE
[MIT](https://github.com/SWM-SparkPlus/db-updater/blob/master/LICENSE)
