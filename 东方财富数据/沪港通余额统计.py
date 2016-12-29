# -*- coding: utf-8 -*-
import urllib2
import pandas as pd
import sqlite3
url='http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=SHT&sty=SHTHPS&p=1&ps=1000&mkt=1'
content=urllib2.urlopen(url).read().replace("(",'').replace(")",'').replace('%','')
content =eval(content)
newdata=[]
for i in content:
    tp=i.split(',')
    newdata.append(tp)
mydf=pd.DataFrame(newdata)
del mydf[1],mydf[6],mydf[7],mydf[8],mydf[9]
mydf.columns=['dates','buyIn','saleOut','netBusIn','balance','SH','SHOverRate']  # 金额单位为：百万
mydf['dates']=pd.to_datetime(mydf['dates'],format='%Y-%m-%d')
mydf=mydf[mydf.dates>'2014-11-20']
mydf.buyIn=mydf.buyIn.astype(float)
mydf.saleOut=mydf.saleOut.astype(float)
mydf.netBusIn=mydf.netBusIn.astype(float)
mydf.balance=mydf.balance.astype(float)
mydf.SH=mydf.SH.astype(float)
conn=sqlite3.connect('e:/360yun/myprog/TestData.db')
mydf.SHOverRate=mydf.SHOverRate.astype(float)
#mydf.reindex(mydf['dates'])
mydf=mydf.set_index(['dates'])
print mydf
mydf.to_sql('HGTbalance',conn,if_exists='replace')