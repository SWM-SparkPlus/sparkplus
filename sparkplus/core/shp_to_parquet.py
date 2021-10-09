import sys
import geopandas as gpd

input_file = sys.argv[1]
file_name = str(input_file)[:-4]
region_code = int(file_name[16:18])
gdf = gpd.read_file(input_file)
dict = {
    41: "Gyeonggi",
    48: "Gyeongnam",
    47: "Gyeongbuk",
    29: "Gwangju",
    27: "Daegu",
    30: "Daejeon",
    26: "Busan",
    11: "Seoul",
    36: "Sejong",
    28: "Incheon",
    26: "Jeonnam",
    45: "Jeonbuk",
    50: "Jeju",
    44: "Chungnam",
    43: "Chungbuk",
    31: "Ulsan",
    42: "Kangwon",
}
file_name = dict[region_code] + ".parquet"
gdf = gdf.set_crs(5174)
gdf = gdf.to_crs(4326)
gdf.to_parquet(file_name)
