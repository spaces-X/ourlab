import pandas as pd
from numpy import nan
from fuzzywuzzy import fuzz

def add(df1,df2,on_left,on_right,on_left_fuzzy,on_right_fuzzy):
    if len(on_left!=on_right):
        print("左右精确合并准则维度不一致")
        raise Exception
    if len(on_left_fuzzy)!=len(on_right_fuzzy):
        print("左右模糊匹配准则维度不一致")
        raise Exception
    possible=[]
    for i in range(len(df1)):
        possible.append([])
        for j in range(len(df2)):
            if df1.iloc[i][[on_left]]!=df2.iloc[j][on_right]:
                continue
            possible[i].append(j)
    new_data_list = []
    for i in range(len(possible)):
        max_similarity=0
        max_sim_index=-1
        if len(possible[i])==0:                                           # 精确合并时无匹配项
            new_data_list.append(df1.iloc[i].tolist().extend([nan]*len(df2.columns)))
            continue
        for index in possible[i]:
            temp_sim = fuzz.partial_ratio(tuple(df1.iloc[i][on_left_fuzzy].values),tuple(df2.iloc[index][on_right_fuzzy].values))
            if temp_sim>max_similarity:
                max_sim_index=index
                max_similarity=temp_sim
        if max_sim_index==-1 or max_similarity<10:                         # 模糊匹配无匹配项
            new_data_list.append(df1.iloc[i].tolist().extend([nan]*len(df2.columns)))
        else:
            new_data_list.append(df1.iloc[i].tolist().extend(df2.iloc[max_sim_index].tolist()))          # 模糊有匹配项 将原有行与匹配行 extend成一个长行
        



        
        



            






main_data = pd.read_csv("交易收费站.csv")

# 合并交易收费站
to_add = pd.read_csv("交易地图匹配.csv",encoding='gbk')
all_data = pd.merge(main_data,to_add,left_on=['STATION_NAME','PROVINCE'],right_on=['statName交易','provName'],how='left').drop_duplicates(subset='ID',keep='first')

# 合并地图收费站
to_add = pd.read_csv("地图收费站.csv")
#to_add['statName'] = to_add['statName'].apply(lambda x:x.strip("收费站")) # 去掉尾部  收费站 
all_data = pd.merge(all_data,to_add,left_on=['statName地图','provName'],right_on=['statName','prov'],how='left').drop_duplicates(subset='ID',keep='first')

# 合并互联网收费站

to_add = pd.read_csv("互联网收费站.csv")
all_data = pd.merge(all_data,to_add,left_on='ID',right_on='ID',how='left')


to_add1 = pd.read_csv("sta_id2sta_type.csv",encoding='gbk')
to_add1 = to_add1[to_add1['STATION_TYPE']!='NOT_PROVINCE_STATION']
to_add2 = pd.read_csv("收费广场(全).csv",encoding='gbk').drop_duplicates(subset='TOLL_STATION_ID',keep='first')
to_add2 = to_add2[~to_add2['TOLL_STATION_ID'].isin(to_add1['ID']) ]
to_add2 = to_add2[['TOLL_STATION_ID','PLAZA_TYPE']]


print(all_data)
print(all_data.shape[0])
all_data.to_csv("收费站信息汇总_gbk.csv",encoding='gbk',index=None)

## test