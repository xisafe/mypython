import math
import os
import pandas as pd
from sqlalchemy import create_engine
code='600866'
engine= create_engine('sqlite:////Users/hua/documents/TestData.db')
stdata = pd.read_sql_query("select * from stocks where code='{0}' and date>'2016-02-01'".format(code),con= engine,index_col='date')
stdata=stdata.sort_index()
stdata[['close','turnover']].plot(figsize=(33,8),secondary_y='close',grid=True)
dateby_sql='''select a.dates as dates,a.num as num,a.vol as vol,b.close as price from(
select date as dates,count(1) num,sum(volume) vol from stocks where code not in('sh','sz') GROUP BY  date) AS a 
INNER JOIN (select date,close from stocks where code='sh') as b on a.dates=b.date'''
#print(dateby_sql)
group_date=pd.read_sql_query(dateby_sql,con= engine,index_col='dates')
group_date=group_date.sort_index()
group_date['vol_avg']=group_date['vol']/group_date['num']
group_date[['vol_avg','price']].plot(figsize=(33,8),secondary_y='vol_avg',grid=True)