#https://zhuanlan.zhihu.com/p/21401793?refer=loan-analytics
import pandas as pd
dt=pd.read_csv('/users/hua/downloads/detail_data7.csv',sep ='\t',encoding='UTF-16LE')
dt['Total2']=dt['Total']-dt['Refund']
#no_mat=dt[dt['Is']==0]
#dt2=dt[dt['HasCaculate']==1]
cost=dt.groupby(by='AccountTurnover').agg({'Distributedcost':sum,'Expense':max,'Total2':sum,'Profit':sum,'Income':sum})
#cost2=dt.groupby(by='AccountTurnover').agg({'Distributedcost':sum,'Expense':max,'Total':sum,'Profit':sum})
cost['netdiff']=cost['Total2']+cost['Expense']
cost['netdiff2']=cost['Distributedcost']+cost['Expense']
st=cost.sum()
npt=dt[pd.isnull(dt['OrderId'])]
dt[dt['AccountTurnover']==301462952067960]
payout=dt[['AccountTurnover','PoNumber','Expense']].drop_duplicates()
p1=payout.groupby(by='PoNumber').agg({'Expense':sum})
mydiff=cost[cost['netdiff2'].apply(lambda x: abs(x)>1)]
mydiff[mydiff.index==301462952067960]
nodiff=cost[cost['netdiff'].apply(lambda x: abs(x)<=1)]
#mydiff.to_csv('/users/hua/out.csv')
op=dt.merge(mydiff,how='inner',left_on='AccountTurnover',right_index=True)
op['UpdateDate']=pd.to_datetime(op['UpdateDate'])
op['PoNumber']=op['PoNumber'].apply(str)
op['AccountTurnover']=op['AccountTurnover'].apply(str)
ty=op[op['UpdateDate']>'2017-05-27']
op[op['AccountTurnover']=='301462952067960']
op.to_excel('/users/hua/out.xlsx',index=False)
"""inlist=list(mydiff.index)
errors=dt[dt['AccountTurnover'].apply(lambda x: x in inlist)]

dt['PoNumber'].to_csv('/users/hua/out2.csv',index=False)
dt['PoNumber'].drop_dup
po=dt[dt['PoNumber']==20783669962859862]
"""
