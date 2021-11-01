from enum import Enum
from typing import List


class EPrefix(Enum):
    """
    Prefixes of Spark+ database.
    Get details from https://github.com/SWM-SparkPlus/db-updater#%EB%8F%84%EB%A1%9C%EB%AA%85%EC%A3%BC%EC%86%8C-%ED%85%8C%EC%9D%B4%EB%B8%94
    """

    ADDINFO = "additional_info"
    ROADNAME = "roadname_address"
    JIBUN = "jibun_address"
    INTEGRATED = "integrated_address"


class ESido(Enum):
    """
    Enum for Korean metropolitan cities.
    """

    SEOUL = "seoul"
    INCHEON = "incheon"
    DAEJEON = "daejeon"
    SEJONG = "sejong"
    GWANGJU = "gwangju"
    DAEGU = "daegu"
    ULSAN = "ulsan"
    BUSAN = "busan"
    JEJU = "jeju"
    GYEONGGI = "gyeonggi"
    GANGWON = "gangwon"
    CHUNGBUK = "chungbuk"
    CHUNGNAM = "chungnam"
    JEONBUK = "jeonbuk"
    JEONNAM = "jeonnam"
    GYEONGBUK = "gyeongbuk"
    GYEONGNAM = "gyeongnam"


def get_tablename_by_prefix_and_sido(prefix: EPrefix, sido: ESido) -> str:
    """
    Get tablename of Spark+ database.

    Example
    --------

    >>> target_table = get_table_name(EPrefix.ADDINFO, ESIDO.SEOUL)    # additional_info_seoul
    >>> target_table = get_table_name(EPrefix.INTEGRATED, ESIDO.BUSAN) # integrated_address_busan
    >>> error_table = get_table_name(EPrefix.ADDINFO,  "anywhere")     # Get AttributeError
    """

    return f"{prefix.value}_{sido.value}"


def get_all_tablenames_by_prefix(prefix: EPrefix) -> List[str]:
    """
    Get all tablenames by given `EPrefix`. If you want to load all database tables to Spark `DataFrame`, see example below.
    It takes a lot of intensive works,

    Example
    -------

    >>> from tablename import get_all_tablenames_by_prefix, EPrefix
    >>> get_all_tablenames_by_prefix(EPrefix.INTEGRATED)
    ['integrated_address_seoul', 'integrated_address_incheon', 'integrated_address_daejeon', 'integrated_address_sejong', 'integrated_address_gwangju', 'integrated_address_daegu', 'integrated_address_ulsan', 'integrated_address_busan', 'integrated_address_jeju', 'integrated_address_gyeonggi', 'integrated_address_gangwon', 'integrated_address_chungbuk', 'integrated_address_chungnam', 'integrated_address_jeonbuk', 'integrated_address_jeonnam', 'integrated_address_gyeongbuk', 'integrated_address_gyeongnam']
    >>> # Load all data from database
    >>> from pyspark.sql import SparkSession
    >>> from pyspark.sql.functions import rand
    >>> from base import SPDataFrame
    >>> ss = SparkSession.builder.config('spark.driver.memory', '14g').getOrCreate()
    >>> all_tablenames = get_all_tablenames_by_prefix(EPrefix.INTEGRATED)
    >>> SPDataFrame.get_db_df_by_tablenames(ss, all_tablenames, ...).select('sido', 'sigungu', 'eupmyeondong').orderBy(rand()).show()
    +----------+-------------+------------+
    |      sido|      sigungu|eupmyeondong|
    +----------+-------------+------------+
    |광주광역시|         동구|   금남로4가|
    |인천광역시|       옹진군|      백령면|
    |  전라북도|전주시 덕진구|   인후동1가|
    |    경기도|용인시 처인구|      포곡읍|
    |  전라남도|       해남군|      화산면|
    |    강원도|       철원군|        서면|
    |    경기도|성남시 수정구|      산성동|
    |  경상남도|       산청군|      금서면|
    |  전라남도|       보성군|      웅치면|
    |  전라남도|       완도군|      약산면|
    |    경기도|       이천시|    장호원읍|
    |    경기도|       포천시|      가산면|
    |    경기도|       부천시|      소사동|
    |  경상남도|       창녕군|      영산면|
    |    강원도|       원주시|      학성동|
    |부산광역시|       강서구|     대저1동|
    |  전라남도|       곡성군|      옥과면|
    |  경상북도|       울진군|        북면|
    |  충청남도|       아산시|      탕정면|
    |서울특별시|       중랑구|      면목동|
    +----------+-------------+------------+
    """
    return [f"{prefix.value}_{sido.value}" for sido in ESido]
