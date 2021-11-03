# SparkPlus
Spark+는 H3, 위/경도 좌표 등의 공간 정보를 국내 주소체계(신주소/구주소)와 함께 처리할 수 있도록 지원하는 Plugin입니다.

Spark+는 Apache Spark와 MySQL을 지원합니다.

## Spark+ 전체 아키텍처

![](https://github.com/SWM-SparkPlus/kr-address-db-updater/blob/master/statics/sparkplus_architecture.png)

## Setup
- Spark+는 PyPI에 배포되어 있으며, 다음 커맨드로 설치할 수 있습니다.
```
$ pip install sparkplus
```

- 설치 후에 import하여 사용할 수 있습니다.
```
from sparkplus.core import CoordDataFrame, RoadnameDataFrame, NumAddrDataFrame
```

[개발자 가이드 참고](https://github.com/SWM-SparkPlus/sparkplus/wiki)

## Class

### CoordDataFrame
```
coord_df = CoordDataFrame(source_df, geo_df, table_df, x_colname, y_colname)
```
### RoadnameDataFrame
```
roadname_df = RoadnameDataFrame(source_df)
```
### NumAddrDataFrame
```
numaddr_df = NumaddrDataFrame(source_df)
```


## LICENSE
[MIT](https://github.com/SWM-SparkPlus/db-updater/blob/master/LICENSE)
