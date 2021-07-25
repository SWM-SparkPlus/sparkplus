#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, LineString
import matplotlib.pyplot as plt
import matplotlib
import matplotlib.font_manager as fm
from matplotlib import font_manager, rc
from pyspark.sql import SparkSession
from geospark.register import GeoSparkRegistrator
from geospark.register import upload_jars


# In[3]:


from pyproj import Transformer
import numpy as np
import pandas as pd


# In[4]:


korea_shp_file = "shp/dong/TL_SCCO_EMD.shp"


# In[5]:


korea = gpd.read_file(korea_shp_file, encoding='euc-kr')


# In[6]:


plt.rcParams["font.family"] = 'NanumGothic'


# In[7]:


korea_wgs = korea.to_crs(4326)


# In[13]:


geumgok = korea_wgs[korea_wgs.EMD_KOR_NM == "금곡동"]


# In[14]:


geumgok


# In[17]:


home_lat = 37.3
home_lng = 127.1
home = Point(home_lng, home_lat)


# In[18]:


geumgok.geometry.contains(home)


# In[ ]:




