#https://zhuanlan.zhihu.com/p/21401793?refer=loan-analytics
import pandas as pd
dt=pd.read_csv('/users/hua/downloads/detail_data.csv',sep ='\t',encoding='UTF-16LE')
cost=dt.groupby(by='AccountTurnover').agg({'Distributedcost':sum,'Expense':max,'Total':sum,'Profit':sum})
cost['netdiff']=cost['Distributedcost']+cost['Expense']
cost.sum()
mydiff=cost[cost['netdiff'].apply(lambda x: abs(x)>0)]
mydiff.to_csv('/users/hua/out.csv')
inlist=list(mydiff[mydiff['Distributedcost']<0].index)
errors=dt[dt['AccountTurnover'].apply(lambda x: x in inlist)]
dt[dt['PoNumber']=='22635369347859862']
