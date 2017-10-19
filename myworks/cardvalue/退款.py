#https://zhuanlan.zhihu.com/p/21401793?refer=loan-analytics
import pandas as pd
import numpy
dt=pd.read_csv('/users/hua/downloads/temp_data_tk.csv',sep ='\t',encoding='UTF-16LE')
dt['ev_ym']=dt['EventTime'].apply(lambda x: x[0:6])
dt['po_ym']=dt['PoPlaceDate'].apply(lambda x: x[0:6])
diffs=dt[dt['ev_ym']!=dt['po_ym']]
st=dt.groupby('ev_ym').agg({'AccountTurnover':'count','Income':'sum'})
no_st=diffs.groupby('ev_ym').agg({'AccountTurnover':'count','Income':'sum'})
no_st.columns=['跨期退款笔数','跨期退款金额']
st=st.join(no_st)
