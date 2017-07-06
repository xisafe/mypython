#https://zhuanlan.zhihu.com/p/21401793?refer=loan-analytics
import pandas as pd
import numpy
dt=pd.read_csv('/users/hua/downloads/detail_data0615.csv',sep ='\t',encoding='UTF-16LE')
#dt['PayOut']=dt['Expense']+dt['Income']
#orders=pd.read_csv('/users/hua/downloads/order_data.csv',sep ='\t',encoding='UTF-16LE')
#st3= orders.sum()
to_map=dt[['OrderId','PoNumber']].drop_duplicates()
dt['Distributedcost']=dt['Distributedcost'].fillna(0)
dt['Expense']=dt['Expense'].fillna(0)
dt['Income']=dt['Income'].fillna(0)
dt['Total']=dt['Total'].fillna(0)
dt['Profit']=dt['Profit'].fillna(0)
ordernum=to_map.groupby(by='OrderId').count()
#dt['Total2']=dt['Total']-dt['Refund']
orders=dt[['OrderId','PoNumbers','PoNumber','Total','Profit','Distributedcost']].drop_duplicates()
#pocost3['diffs']=pocost3['Total']-pocost3['Distributedcost']-pocost3['Profit']
pocost2=dt[['OrderId','PoNumber','Total','Profit','Distributedcost']].drop_duplicates()
ordernum=to_map.groupby(by='OrderId').count()
ordernum.columns=['PoNums']
#pocost2[pocost2['OrderId']==29326726]#'PoNumber'
#pocost3=pocost3.merge(orders,how='left',left_on='OrderId',right_on='OrderId')
#st3=pocost3.sum()
#maped=pocost3.merge(to_map,how='left',left_on='OrderId',right_on='OrderId')
#dt[dt['OrderId']==28740399]
#tpz=pocost3[pocost3['OrderId'].apply(lambda x:x not in orlist)]
pocost=pocost2.groupby(by='PoNumber').agg({'Distributedcost':sum,'Total':sum,'Profit':sum})
po=dt[['PoNumber','AccountTurnover','Expense','Income']].drop_duplicates()
po['PayOut']=po['Expense']+po['Income']
po=po.groupby(by='PoNumber').agg({'AccountTurnover':numpy.size,'PayOut':sum})
po=po.join(pocost)
del po['AccountTurnover']
po['diffs']=po['Distributedcost']+po['PayOut']
#maped=maped.merge(po,how='left',left_on='PoNumber',right_index=True)
#maped['diffs']=maped['Distributedcost']+maped['PayOut']
#po[po.index==23054808258859862]
#dt[dt['PoNumber']==13411041405269603]
st=po.sum()
mydiff=po[po['diffs'].apply(lambda x: abs(x)>=2 or pd.isnull(x))]
mydiffst=mydiff.sum()
nodiff=po[po['diffs'].apply(lambda x: abs(x)<2)]
nodiff_op=dt.merge(nodiff,how='inner',left_on='PoNumber',right_index=True)
nodiff_op_out=nodiff_op[nodiff_op.Profit_x<-1]
nodiff_op_out['UpdateDate']=pd.to_datetime(nodiff_op_out['UpdateDate'])
nodiff_op_out['PoNumber']=nodiff_op_out['PoNumber'].apply(str)
nodiff_op_out['AccountTurnover']=nodiff_op_out['AccountTurnover'].apply(str)
nodiff_op_out.to_excel('/users/hua/nodiff_out.xlsx',index=False)
#mydiff=mydiff.merge(to_map,how='left',left_index=True,right_on='PoNumber')
op=dt.merge(mydiff,how='inner',left_on='PoNumber',right_index=True)
op=op.merge(ordernum,how='left',left_on='OrderId',right_index=True)
ordersum=op.groupby(by='OrderId').agg({'PayOut':sum,'Distributedcost_x':max})
ordersum['diffs']=ordersum['Distributedcost_x']+ordersum['PayOut']
ordersum=ordersum[ordersum['diffs'].apply(lambda x: abs(x)>=1 or pd.isnull(x))]
del ordersum['PayOut'],ordersum['diffs']
ordersum.columns=['Distributedcost_order']
op=op.merge(ordersum,how='inner',left_on='OrderId',right_index=True)
diff2=op[['PoNumbers','Distributedcost_x','OrderId','PayOut']].drop_duplicates()
diff2=diff2.groupby(by=['PoNumbers']).agg({'Distributedcost_x':sum,'PayOut':max})
diff2['diffs']=diff2['Distributedcost_x']+diff2['PayOut']
diff2=diff2[diff2['diffs'].apply(lambda x: abs(x)>1 or pd.isnull(x))]
op=op.merge(diff2,how='inner',left_on='PoNumbers',right_index=True)
del op['记录数'],op['IsNToN']
#cost=dt.groupby(by='AccountTurnover').agg({'Distributedcost':sum,'Expense':max,'Total2':sum,'Profit':sum,'Income':sum})
#payout=dt[['AccountTurnover','PoNumber','Expense']].drop_duplicates()
#p1=payout.groupby(by='PoNumber').agg({'Expense':sum})
#mydiff=cost[cost['netdiff2'].apply(lambda x: abs(x)>1)]
#mydiff[mydiff.index==301462952067960]
#nodiff=cost[cost['netdiff'].apply(lambda x: abs(x)<=1)]
#mydiff.to_csv('/users/hua/out.csv')
op['UpdateDate']=pd.to_datetime(op['UpdateDate'])
op['PoNumber']=op['PoNumber'].apply(str)
op['AccountTurnover']=op['AccountTurnover'].apply(str)
#ty=op[op['UpdateDate']>'2017-05-27']
#op[op['AccountTurnover']=='301462952067960']
op.to_excel('/users/hua/outfiles/out.xlsx',index=False)
#mydiff['PoNumber']=mydiff['PoNumber'].apply(str)按亏损金额 按亏损数量
#mydiff.to_excel('/users/hua/out_diff.xlsx',index=False)
#db.getCollection('model.Order').find({OrderId:27307608}).pretty()
#db.getCollection('model.TopTrade').find({PoNumber:'16012063425445444'}).pretty()
#db.getCollection('model.TopTrade').find({OrderId:28785023}).pretty()
#db.getCollection('model.Order').find({PoNumber:'12291831785222496'}).pretty()
#db.getCollection('model.PurchaseTradeRecord').find({PoNumber:'16012063425445444'}).pretty()
