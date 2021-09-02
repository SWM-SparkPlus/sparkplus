import pandas as pd
from pandas import DataFrame

additional_info_tables = [
	'additional_info_busan',
	'additional_info_chungbuk',
	'additional_info_chungnam',
	'additional_info_daegu',
	'additional_info_daejeon',
	'additional_info_gangwon',
	'additional_info_gwangju',
	'additional_info_gyeongbuk',
	'additional_info_gyeonggi',
	'additional_info_gyeongnam',
	'additional_info_incheon',
	'additional_info_jeju',
	'additional_info_jeonbuk',
	'additional_info_jeonnam',
	'additional_info_sejong',
	'additional_info_seoul',
	'additional_info_ulsan'
]

jibun_address_tables = [
	'jibun_address_busan',
	'jibun_address_chungbuk',
	'jibun_address_chungnam',
	'jibun_address_daegu',
	'jibun_address_daejeon',
	'jibun_address_gangwon',
	'jibun_address_gwangju',
	'jibun_address_gyeongbuk',
	'jibun_address_gyeonggi',
	'jibun_address_gyeongnam',
	'jibun_address_incheon',
	'jibun_address_jeju',
	'jibun_address_jeonbuk',
	'jibun_address_jeonnam',
	'jibun_address_sejong',
	'jibun_address_seoul',
	'jibun_address_ulsan',
]

tables = [
	'roadname_address_busan',
	'roadname_address_chungbuk',
	'roadname_address_chungnam',
	'roadname_address_daegu',
	'roadname_address_daejeon',
	'roadname_address_gangwon',
	'roadname_address_gwangju',
	'roadname_address_gyeongbuk',
	'roadname_address_gyeonggi',
	'roadname_address_gyeongnam',
	'roadname_address_incheon',
	'roadname_address_jeju',
	'roadname_address_jeonbuk',
	'roadname_address_jeonnam',
	'roadname_address_sejong',
	'roadname_address_seoul',
	'roadname_address_ulsan',
    #'roadname_code'
]


def load_tables(spark, url, user, password, driver="com.mysql.cj.jdbc.Driver", opt=0):
    if opt == 0:
        result = spark.read.format('jdbc') \
                                .option('driver', driver) \
                                .option('url', url) \
                                .option('dbtable', tables[0]) \
                                .option('user', user) \
                                .option('password', password) \
                                .load()
        for table in tables[1:]:
            temp = spark.read.format('jdbc') \
                                .option('driver', driver) \
                                .option('url', url) \
                                .option('dbtable', table) \
                                .option('user', user) \
                                .option('password', password) \
                                .load()
            result = result.union(temp)
    else:
        table = opt
        result = spark.read.format('jdbc') \
                                .option('driver', driver) \
                                .option('url', url) \
                                .option('dbtable', table) \
                                .option('user', user) \
                                .option('password', password) \
                                .load()
    return result