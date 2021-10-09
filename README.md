# SparkPlus
Spark+는 H3, 위/경도 좌표 등의 공간 정보를 국내 주소체계(신주소/구주소)와 함께 처리할 수 있도록 지원하는 Plugin입니다.

Spark+는 Apache Spark와 MySQL을 지원합니다.

### dependecnies
- spark
   - `start_spark(app_name, master, jar_packages, files, spark_config)`
      ```
      from dependecnies.spark import start_spark

      spark, *_ = start_spark
      ```
      - spark session을 시작합니다.
      - params
         - app_name: str, 어플리케이션 이름(default: my_spark_app)
         - master: str, master 노드(default: local[*])
         - jar_packages: list, Spark Jar package 이름
         - files: list, Spark Cluster(master/worker)로 전송할 파일
         - spark_config: dict, config key-value pairs
      - return
         - Spark session
         - logger
         - config dict

- logging
   - `Log4j(object: spark session)`


### jobs
- load_database
   - `load_tables(spark, url, user, password, driver, opt)`
      ```
      from jobs.load_tables import load_tables

      driver = "driver"
      url = "url"
      user = "user"
      password = "password"
      opt = "seoul"

      df = load_tables(spark, url, user, password, driver, opt)
      df.show()
      ```
      - 데이터베이스에서 테이블을 불러옵니다
      - params
         - spark: obj, spark session
         - url: str, db url
         - user: str, db user
         - password: str, db password
         - driver: str, mysql driver(default: "com.mysql.cj.jdbc.Driver"
         - opt: str, 불러올 테이블 지역(default: "all")
- conversion
  - `join_with_emd(shp_gdf, spark_df, x_colname, y_colname)`
    - params
      - shp_gdf: geo-dataframe으로 불러온 shp
      - spark_df: 위/경도 좌표를 포함하는 spark-dataframe
      - x_colname: 경도 column name, str
      - y_colname: 위도 column name, str
    - return
      - 위/경도 좌표와 법정동 코드가 맵핑된 spark-dataframe
  - `join_with_h3(spark_df, x_colname, y_colname, h3_level)`
    - params
      - spark_df: 위/경도 좌표를 포함하는 spark-dataframe
      - x_colname: 경도 column name, str
      - y_colname: 위도 column name, str
      - h3_level: h3 레벨, int
    - return
      - 위/경도 좌표와 h3 정보가 맵핑된 spark-dataframe
  - `join_with_table(shp_gdf, spark_df, table_df, x_colname, y_colname)`
    - params
      - shp_gdf: geo-dataframe으로 불러온 shp
      - spark_df: 위/경도 좌표를 포함하는 spark-dataframe
      - table_df: 주소데이터베이스에서 불러온 지역별 통합 테이블 spark-dataframe
      - x_colname: 경도 column name, str
      - y_colname: 위도 column name, str
    - return
      - 위/경도 좌표와 법정동 코드, 주소정보가 맵핑된 spark-dataframe

### package
- /gis
   - `gis_init()`
   - `coord_to_dong(spark, gdf, spark_df, lng_colname, lat_colname)`
