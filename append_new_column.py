#! /usr/bin/env python
# -*- coding: utf-8 -*-
'''
这个文件是在获取了“收费站信息汇总表”后
这个表是来自多个源的，我们要根据这些多源来对原有
的收费站信息进行补充（如某些源缺失经纬度信息）和确定使用某一信息源（交易数据or电子地图or互联网）
当确定使用某一个信息源后 将最终确定的信息加到最后几列
并在最后一列中注明信息来源

'''
import pandas as pd
from numpy import nan

data = pd.read_csv('收费站信息汇总.csv')
# data = pd.concat([data,pd.DataFrame(columns=['STATION_ID','STATION_NAME','STATION_PRO','STATION_TYPE','STATION_LNG','STATION_LAT'])])
data['STATION_ID']=data['ID']
data['STATION_Name']=data['STATION_NAME']
data['STATION_PRO']=data['PROVINCE']
to_add = pd.read_csv("station_id2type.csv")
data = pd.merge(data,to_add,how='left',on='ID')
data['STATION_LNG']=data['lng_y']
data['STATION_LAT']=data['lat_y']
data['DATA_FROM_SOURCE'] = 0                  # 默认选取掌上通数据
index1 = data['STATION_LNG'].isnull() | data['STATION_LAT'].isnull()
data.loc[index1,['STATION_LNG']] = data[index1]['slng']
data.loc[index1,['STATION_LAT']] = data[index1]['slat']
data.loc[index1,['DATA_FROM_SOURCE']] = 1     # 1代表收费站交易数据

index2 = data['STATION_LNG'].isnull() | data['STATION_LAT'].isnull()
data.loc[index2,['STATION_LNG']] = data[index2]['lng_x']
data.loc[index2,['STATION_LAT']] = data[index2]['lat_x']
data.loc[index2,['DATA_FROM_SOURCE']] = 2  

index3 = data['STATION_LNG'].isnull() | data['STATION_LAT'].isnull()
data.loc[index3,['DATA_FROM_SOURCE']] = 3     # 3代表 没有地理信息

data.to_csv("收费站信息汇总(全).csv",index=None)
data.to_csv("收费站信息汇总(全)_gbk.csv",index=None,encoding='gbk')