# 画词云图所需要的函数
import pandas as pd
from pandas import DataFrame,Series
import numpy as np
import jieba

class ciyuntu_class:
    # 字符串分词
    def fenci(self,sentence):
        seg_list = list(jieba.cut(sentence))
        # 去除标点符号
        flag_list = ['。','、','，','？','！','；','‘','’','“','”']
        word_list = ['的','很','了','也','穿','是','买','还','和']
        flag_list.extend(word_list)
        seg_list = np.array(seg_list)
        flag_list = np.array(flag_list)
        for i in flag_list:
            seg_list = seg_list[seg_list != i]
        seg_list = list(seg_list)
        return(seg_list)
    # 列表元素频率统计，返回数据框，columns:word,freq
    def sta_list(self,lst):
        lst = np.array(lst)
        lst_unique = list(set((lst)))
        # df = DataFrame(lst_unique)
        # df.columns = ['word']
        freq = []
        for item in lst_unique:
            freq.append(sum(lst == item))
        df = DataFrame(freq)
        df.columns = ['freq']
        df.index = lst_unique
        return(df)
    # 数据框合并
    def bind_df(self,df1,df2):
        df3 = df1+df2
        freq = df3['freq']
        freq_index = df3.index
        nan_index = freq_index[np.isnan(np.array(freq))]
        for i in nan_index:
            if i in df1.index:
                freq[i] = df1.ix[i]
            else:
                freq[i] = df2.ix[i]
        df3['freq'] = freq
        df3 = df3.applymap(lambda x:int(x))
        return(df3)
    # 数据框转换成元组列表
    def df2tuple_list(self,df):
        # 按freq降序排列
        df = df.sort_values(by='freq',ascending=False)
        if df.shape[0] >= 50:
            df = df.ix[0:50]
        tuple_list = []
        for i in range(df.shape[0]):
            tuple_list.append((df.index[i],df['freq'][i]))
        return(tuple_list)




