# SparkPlus
Spark+는 H3, 위/경도 좌표 등의 공간 정보를 국내 주소체계(신주소/구주소)와 함께 처리할 수 있도록 지원하는 Plugin입니다.

Spark+는 Apache Spark와 MySQL을 지원합니다.

## Setup
---
Spark+는 Pypi에 배포되어 있으며, 다음 커맨드로 설치할 수 있습니다.
```
pip install sparkplus
```

## Usage
---
```
from sparkplus.core import CoordDataframe, RoadnameDataframe
```
### CoordDataframe
```
df = CoordDataframe(source_df, gdf, table_df, x_colname, y_colname)
```
- params
  - source_df: 위/경도 좌표를 포함한 원본 Spark Dataframe
  - gdf: `shp`, `parquet` 로부터 생성한 Geo Dataframe
  - table_df: 주소 데이터베이스로부터 생성한 Spark Dataframe
  - x_colname, y_colname: 원본 Spark Dataframe의 위/경도 좌표

**coord_to_pnu()**
```
df.coord_to_pnu()
```
- 해당 좌표를 포함하는 PNU코드 컬럼 추가

**coord_to_h3()**
```
df.coord_to_h3()
```
- 해당 좌표를 포함하는 h3 컬럼 추가

**coord_to_zipcode()**
```
df.coord_to_zipcode()
```
- 해당 좌표를 포함하는 우편번호 컬럼 추가

**coord_to_emd()**
```
df.coord_to_emd()
```
- 해당 좌표를 포함하는 법정동코드 컬럼 추가

**coord_to_roadname()**
```
df.coord_to_roadname()
```
- 해당 좌표를 포함하는 도로명 주소(시도, 시군구, 도로명, 읍면동, 법정리, 지하여부, 건물번호 1, 건물번호 2) 컬럼 추가

**coord_to_roadname_addr()**
```
df.coord_to_roadname_addr()
```
- 해당 좌표를 포함하는 전체 도로명 주소 컬럼 추가

**coord_to_jibun()**
```
df.coord_to_jibun()
```
- 해당 좌표를 포함하는 지번주소(시도, 시군구, 읍면동, 법정리, 지번 1, 지번 2) 컬럼 추가

**join_with_table()**
```
df.join_with_table()
```
- 원본 소스 데이터프레임과 주소 데이터베이스에서 가져온 테이블 조인


### RoadnameDataframe
---

