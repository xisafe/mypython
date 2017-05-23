import pandas as pd
#import pymysql
#onn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='root',db='test',charset='utf8')
turnover=pd.read_csv('/users/hua/downloads/流水20170316.csv',sep ='\t',encoding='UTF-16LE')

turnover['nettotal']=turnover['Income']+turnover['Expense']

neizhuan=turnover[turnover['EzbuyBusinessType']=='内部转账']
turnover_other=turnover[turnover['EzbuyBusinessType']=='交易退款']
turnover=turnover[turnover['EzbuyBusinessType'].apply(lambda x :x in['在线支付'])]
turnover_matched=turnover[turnover['IsMatched']==1]
turnover_unmatched=turnover[turnover['IsMatched']==0]
turnover_matched['Total']=turnover_matched['Total']-turnover_matched['Refund']
matched_cost=pd.DataFrame(turnover_matched.groupby(by=['AccountTurnover'])['Total','Distributedcost'].sum())
turnover_total=turnover_matched[['AccountTurnover','nettotal']].drop_duplicates()
print(sum(turnover_total.nettotal))
print(sum(turnover_matched.Total))
print("流水未匹配订单金额",sum(turnover_matched.nettotal))
turnover_matched_no_order=turnover_matched[turnover_matched['OrderId'].isnull()]
turnover_matched['CreateDate']=pd.to_datetime(turnover_matched['CreateDate'])
turnover_matched['EventTime']=pd.to_datetime(turnover_matched['EventTime'])#.astype(pd.DatetimeIndex)
本期流水分摊至其他期=turnover_matched[(turnover_matched['CreateDate']<'2017-03-02') | (turnover_matched['CreateDate']>'2017-03-08 23:59:59')]
#其他期流水分摊至本期=order_matched[(order_matched['EventTime']<'2017-03-02') | (order_matched['EventTime']>'2017-03-08 23:59:59')]
print(sum(本期流水分摊至其他期.nettotal))
#其他期流水分摊至本期_dis=其他期流水分摊至本期[['OrderId','IsDistributedcost','Distributedcost','Total','Refund']].drop_duplicates()
#print(sum(其他期流水分摊至本期_dis.Distributedcost))
turnover_matched['diff']=abs(turnover_matched['nettotal']+turnover_matched['Total'])

#turnover_matched.to_excel('/Users/hua/documents/data.xlsx')
turnover_matched_diff=turnover_matched[turnover_matched['diff']>1]
turnover_matched_diff['diff']=abs(turnover_matched_diff['nettotal']-turnover_matched_diff['Total'])
turnover_matched_diff=turnover_matched_diff[turnover_matched_diff['diff']>1]
turnover_matched_diff['diff']=abs(turnover_matched_diff['nettotal']+turnover_matched_diff['Total'])
data_merge=matched_cost.merge(turnover_total,how='inner',left_index=True,right_on='AccountTurnover')
data_merge['diff']=data_merge.nettotal+data_merge.Total
err_data=data_merge[data_merge['diff']<-1]
#turnover[turnover['AccountTurnover']==300794424676530]