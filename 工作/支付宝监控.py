import pandas as pd
#import pymysql
#onn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='root',db='test',charset='utf8')
a_1=pd.read_csv('/users/hua/downloads/cost_data/A_1.csv')
a_2=pd.read_csv('/users/hua/downloads/cost_data/A_2.csv')
a_3=pd.read_csv('/users/hua/downloads/cost_data/A_3.csv')
a_4=pd.read_csv('/users/hua/downloads/cost_data/A_4.csv')
a_5=pd.read_csv('/users/hua/downloads/cost_data/A_5.csv')
b_1=pd.read_csv('/users/hua/downloads/cost_data/B_1.csv')
b_2=pd.read_csv('/users/hua/downloads/cost_data/B_2.csv')
b_3=pd.read_csv('/users/hua/downloads/cost_data/B_3.csv')
b_4=pd.read_csv('/users/hua/downloads/cost_data/B_4.csv')
b_5=pd.read_csv('/users/hua/downloads/cost_data/B_5.csv')
order=pd.concat([a_1,a_2,a_3,a_4,a_5])
turnover=pd.concat([b_1,b_2,b_3,b_4,b_5])
del a_1,a_2,a_3,a_4,a_5,b_1,b_2,b_3,b_4,b_5
sumby=order.groupby(by=['IsDistributedcost']).sum()
turnover['nettotal']=turnover['Income']+turnover['Expense']
order['nettotal']=order['Income']+order['Expense']
dis_order=order[['OrderId','IsDistributedcost','Distributedcost','Total','Refund']].drop_duplicates()
tp=order[order.OrderId==25242727]
dis_order_sum=dis_order.groupby(by=['IsDistributedcost']).sum()
neizhuan=turnover[turnover['EzbuyBusinessType']=='内部转账']
turnover_other=order[order['EzbuyBusinessType']=='其他转账']
turnover=turnover[turnover['EzbuyBusinessType'].apply(lambda x :x in['在线支付','交易退款'])]
turnover_matched=turnover[turnover['IsMatched']==1]
turnover_unmatched=turnover[turnover['IsMatched']==0]
count=turnover.groupby(by=['AccountTurnover'])['AccountTurnover'].count()
order_matched=order[order['IsDistributedcost']==1]
turnover_total=turnover[['AccountTurnover','nettotal']]
order_cost=order_matched[['AccountTurnover','Distributedcost']]
print("",sum(order_cost['Distributedcost']))
print("流水未匹配订单金额",sum(turnover_matched.nettotal))
turnover_matched['CreateDate']=pd.to_datetime(turnover_matched['CreateDate'])
turnover_matched['EventTime']=pd.to_datetime(turnover_matched['EventTime'])#.astype(pd.DatetimeIndex)
本期流水分摊至其他期=turnover_matched[(turnover_matched['CreateDate']<'2017-03-02') | (turnover_matched['CreateDate']>'2017-03-08 23:59:59')]
其他期流水分摊至本期=order_matched[(order_matched['EventTime']<'2017-03-02') | (order_matched['EventTime']>'2017-03-08 23:59:59')]
print(sum(本期流水分摊至其他期.nettotal))
其他期流水分摊至本期_dis=其他期流水分摊至本期[['OrderId','IsDistributedcost','Distributedcost','Total','Refund']].drop_duplicates()
print(sum(其他期流水分摊至本期_dis.Distributedcost))