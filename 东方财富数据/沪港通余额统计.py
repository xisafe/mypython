# -*- coding: utf-8 -*-
import urllib
import pandas as pd
import sqlite3
import os
url='http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=SHT&sty=SHTHPS&p=1&ps=1000&mkt=1'
content=urllib.request.urlopen(url).read().decode('utf-8').replace("(",'').replace(")",'').replace('%','')
content =eval(content)
newdata=[]
for i in content:
    tp=i.split(',')
    newdata.append(tp)
mydf=pd.DataFrame(newdata)
del mydf[1],mydf[6],mydf[7],mydf[8],mydf[9]
mydf.columns=['dates','buyIn','saleOut','netBuyIn','balance','SH','SHOverRate']  # 金额单位为：百万
#mydf['dates']=pd.to_datetime(mydf['dates'],format='%Y-%m-%d')
mydf=mydf[mydf.dates>'2014-11-20']
mydf.buyIn=mydf.buyIn.astype(float)/100
mydf.saleOut=mydf.saleOut.astype(float)/100
mydf.netBuyIn=mydf.netBuyIn.astype(float)/100
mydf.balance=mydf.balance.astype(float)/100
mydf.SH=mydf.SH.astype(float)
conn=sqlite3.connect('e:/360yun/myprog/TestData.db')
mydf.SHOverRate=mydf.SHOverRate.astype(float)
#mydf.reindex(mydf['dates']) print(os.getcwd())
mydf=mydf.set_index(['dates'])
#mydf.to_csv('files/hugutong.csv')
#mydf.to_sql('HGTbalance',conn,if_exists='replace')
mydf=mydf.sort_index()
#print(mydf)
mydf=mydf[mydf.index>'2015-11-20']
mydf[['netBuyIn','SH']].plot(secondary_y='SH',figsize=(50,8),grid=True)
