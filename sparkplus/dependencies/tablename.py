from enum import Enum


class EPrefix(Enum):
    ADDINFO = "additional_info"
    ROADNAME = "roadname_address"
    JIBUN = "jibun_address"
    INTEGRATED = "integrated_address"


class ESido(Enum):
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

    ## Examples

    >>> target_table = get_table_name(EPrefix.ADDINFO, ESIDO.SEOUL)    # additional_info_seoul
    >>> target_table = get_table_name(EPrefix.INTEGRATED, ESIDO.BUSAN) # integrated_address_busan
    >>> error_table = get_table_name(EPrefix.ADDINFO,  "anywhere")     # Get AttributeError
    """

    return f"{prefix.value}_{sido.value}"
