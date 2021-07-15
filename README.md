# Spark Plugin

Apache Spark Plugin to analyze Korea's address system

## Spark 개발 환경
### sbt in IntelliJ
- build.sbt
  ```
  name := "sample-scala"

  version := "0.1"

  scalaVersion := "2.12.10"

  val sparkVersion = "3.1.2"

  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion
  )
  ```
- packaging
  - sbt shell에서 `package`
- spark-submit
  - terminal에서 `spark-submit target/scala-2.12/sample-scala_2.12-0.1.jar README.md`

### Spark-submit 옵션
```
./bin/spark-submit \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]
```
- 옵션
  - --class: applicaiton 의 entry point

  - --master: cluster의 master URL

  - --deploy-mode: (cluster)worker nodes에 배포를 할지, (client)로컬에 external client에 배포를 할지

    - default 는 client

  - --conf: 임의 spark 설정 property( key=value format )

    - value 에 공백이 포함 된 경우 "key=value" 처럼 따옴표로 싸야 한다.

  - --jars: 필요 jar 파일들을 포함하여 클러스터에 전달 해준다.

    - 콤마( , ) 로 URL들을 나열해야 한다.

  - executor node에 각 Spark Context 를 위한 작업 디렉터리에 Jar파일들이 복사가 되기 때문에 시간이 지날수록 많은 양의 공간을 차지 하게 되는데, Spark standalone을 사용한다면 automatic cleanup을 설정해야한다.
    - spark.worker.cleanup.appDataTtl property
    - YARN에서는 자동으로 cleanup 해준다. 
