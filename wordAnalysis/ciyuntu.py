######################################
# 说明一下，公众号里的源码都是小编原创的，仅供各位童鞋学习交流。
# 由于小编平时事比较多，好忙好方 T_T,如果各位童鞋对源码有什么疑问
# 的地方，可以在微店里购买答疑券支持下小编^_^，小编一定会非常用
# 心地为各位童鞋解答不懂的地方^_^^_^
######################################
import pandas as pd
from pandas import DataFrame,Series
import numpy as np
import jieba
from ciyuntu_class import ciyuntu_class
cyt = ciyuntu_class()
from pytagcloud import make_tags,create_tag_image
from random import sample
import sqlite3
conn = sqlite3.connect("E:/360yun/myprog/TestData.db") #
comment=pd.read_sql('SELECT * FROM stockBBS LIMIT 10000',conn)
#comment = pd.read_csv('电商评论数据.csv',encoding='gbk')
#comment = comment.drop(['Unnamed: 0'],axis=1)
# df_comment = comment[['评价内容']].ix[0:30]
index_5000 = sample(list(comment.index),5000)
df_comment = comment[['contents']].ix[index_5000]
df_comment.index = range(df_comment.shape[0])
# 分词
# 分词
df_freq = cyt.fenci(df_comment.ix[0][0])
# 转换成数据框
df_freq = cyt.sta_list(df_freq)
for i in range(df_comment.shape[0])[1:]:
    print(i)
    try:
        df_freq0 = cyt.fenci(df_comment.ix[i][0])
        df_freq0 = cyt.sta_list(df_freq0)
        # 合并数据框
        df_freq = cyt.bind_df(df_freq,df_freq0)
    except:
        print(df_comment.ix[i][0])
        pass
# 画词云图
tuple_list = cyt.df2tuple_list(df_freq)
tags = make_tags(tuple_list,maxsize=80)
create_tag_image(tags,'comment_cloud.png',size=(900,600),fontname='simhei')
print(1)