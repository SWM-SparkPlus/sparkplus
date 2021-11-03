# SparkPlus
Spark+는 H3, 위/경도 좌표 등의 공간 정보를 국내 주소체계(신주소/구주소)와 함께 처리할 수 있도록 지원하는 Plugin입니다.

Spark+는 Apache Spark와 MySQL을 지원합니다.

## Spark+ 전체 아키텍처

![](https://github.com/SWM-SparkPlus/kr-address-db-updater/blob/master/statics/sparkplus_architecture.png)

## Setup


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
```
coord_df = CoordDataFrame(source_df, geo_df, table_df, x_colname, y_colname)
```
### RoadnameDataFrame
```
roadname_df = RoadnameDataFrame(source_df)
```
 |target                                  |sido  |sigungu    |roadname |building_primary_number|bupjungdong_code|
 |----------------------------------------|------|-----------|---------|-----------------------|----------------|
 |경기도 안산시 단원구 해봉로 137                |경기도 |안산시 단원구 |해봉로      |137                    |4128112400     |
 |경기도 수원시 장안구 경수대로 1079             |경기도  |수원시 장안구 |경수대로    |1079                   |4128111800     |
 |경기도 안산시 상록구 양달말길 93-7             |경기도  |안산시 상록구 |양달말길    |93                     |4128101100     |
       
### NumAddrDataFrame
```
numaddr_df = NumaddrDataFrame(source_df)
```


## LICENSE
[MIT](https://github.com/SWM-SparkPlus/db-updater/blob/master/LICENSE)
