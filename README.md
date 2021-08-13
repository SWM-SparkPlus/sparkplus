# Spark Plus
Apache Spark Plugin to analyze Korea's address system
Spark+는 대규모 공간 정보를 국내 주소체계(신주소/구주소)와 함께 처리할 수 있도록 지원하는 Plugin입니다.

Spark+는 Apache Spark와 MySQL을 지원합니다.

## Structure
- spark-plus: 패키지 폴더
- setup.py: 패키지의 설정, 서문
- .pypirc: 패키지 업로드를 위한 설정
- 추가: MANIFEST.in 등의 파일

### dependecnies
- /spark
   - `start_spark(app_name, master, jar_packages, files, spark_config)`: spark session을 시작합니다.

- /logging
   - `Log4j(object: spark session)`

### jobs
- /etl_job
- /table_to_df
   - `create_df(spark, table)`: db로부터 DataFrame을 생성합니다.

### package 
- /gis
   - `gis_init()`
   - `coord_to_dong(spark, gdf, spark_df, lng_colname, lat_colname)`

### testjob

### resource
- /EMD_202101: 법정동 정보를 담고 있는 shp 디렉토리입니다.

## Constructor

### GeomFromWKT
- `GeomFromWKT(Wkt: string)`
### GeomFromWKB **
- `GeomFromWKB(Wkb: string)`
### GeomFromGeoJSON
- `GeomFromGeoJSON(GeoJson: string)`

### DfFromSHP

### GeomFromJUSO

### JusoContainsGeom

### Point
- `Point(X: decimal, Y: decimal)`
### PorintFromText
- `PointFromText(Text: string, Delimiter: char)`
### PolygonFromText
- `PolygonFromText(text: string, Delimiter: char)`
### LineStringFromText
- `LineStringFromText(text: string, Delimiter: char)`
### PolygonFromEnvelope **
- `PolygonFromEnvelope(MinX: decimal, MinY: decimal, MaxX: decimal, MinX: decimal)`

## 패키지 빌드 및 업로드
1. `setuptools`, `wheel` 설치
   - `pip install setuptools wheel`
   - `python -m pip install --user --upgrade setuptools wheel`
  
2. 프로젝트 폴더에서 패키지 빌드
   - `python setup.py sdist bdist_wheel`
   - dist 폴더에 `.tar.gz`, `.whl` 파일 생성되었는지 확인

3. PyPi에 패키지 업로드
   - `pip install twine`
   - `python -m twine upload dist/*`
      - PyPi username과 password 입력
