import pandas as pd

main_data = pd.read_csv("交易收费站.csv")

# 合并交易收费站
to_add = pd.read_csv("交易地图匹配.csv",encoding='gbk')
all_data = pd.merge(main_data,to_add,left_on=['STATION_NAME','PROVINCE'],right_on=['statName交易','provName'],how='left').drop_duplicates(subset='ID',keep='first')

# 合并地图收费站
to_add = pd.read_csv("地图收费站.csv")
to_add['statName'] = to_add['statName'].apply(lambda x:x.strip("收费站")) # 去掉尾部  收费站 
all_data = pd.merge(all_data,to_add,left_on=['STATION_NAME','PROVINCE'],right_on=['statName','prov'],how='left').drop_duplicates(subset='ID',keep='first')

# 合并互联网收费站

to_add = pd.read_csv("互联网收费站.csv")
all_data = pd.merge(all_data,to_add,left_on='ID',right_on='ID',how='left')

print(all_data)
print(all_data.shape[0])
all_data.to_csv("收费站信息汇总_gbk.csv",encoding='gbk',index=None)

## test