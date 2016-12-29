# coding=utf-8
import sqlite3
import pandas as pd
import tushare as ts
from lxml import etree
import urllib2
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import math
import tushare as ts
def getBaduser(stockcode=''): #庄托信号
    conn = sqlite3.connect("E:/360yun/myprog/TestData.db") #
    badsql="""select h.userName,h.userUrl,h.cmNum,h.cmDays,k.totaldays,
    round(h.cmNum*1.0/k.totaldays,2) day_avg_cmNum,
    round(h.cmDays*1.0/k.totaldays,2) rate_cmDays,
    round(h.cmNum*1.0/h.cmDays,2) cmday_avg_cmNum
    from(
    SELECT userName ,userUrl,count(1) cmNum,
    count(DISTINCT dates)  cmDays FROM stockBBS{0}
    where userUrl>''
    GROUP BY userName,userUrl HAVING count(1)>3 ) h 
    left join
    (select count(1) totaldays from(SELECT  dates totaldays from stockBBS{0} GROUP BY dates HAVING count(1)>40)) k
    where round(h.cmNum*1.0/k.totaldays,2)>0.3 and round(h.cmDays*1.0/k.totaldays,2) >0.1 and round(h.cmNum*1.0/h.cmDays,2)>2
    ORDER BY h.cmNum desc;""".format(stockcode)
    badusers=pd.read_sql(badsql,conn)
    print badsql
    badusers.columns=[u'评论人',u'评论人链接',u'总评论数',u'参与评论天数',u'总天数',u'日均评论数',u'评论参与度',u'参与日日均评论数']
    return badusers
def PriceAndBBs(stockcode=''): #评论与股价关系
    conn = sqlite3.connect("E:/360yun/myprog/TestData.db") #
    #badsql="""SELECT date(max(dates),'start of day','-1 day') maxdate FROM stockBBS{0}""".format(stockcode)
    ssql="""SELECT substr(dates,0,11) dates,count(1) num FROM stockBBS{0} GROUP BY substr(dates,0,11)""".format(stockcode)
    datesNum=pd.read_sql_query(ssql,conn,index_col='dates')
    price=ts.get_hist_data(stockcode,datesNum.index.min(),datesNum.index.max())
    rs=price.join(datesNum)[['close','num']].fillna(0) #,'price_change'
    rs=rs.sort_index()
    rs.columns=[u'price',u'bbsNum']
    rs.plot(secondary_y=u'bbsNum',figsize=(12,5))
    return rs    
#datesNum=getBaduser(u'600339')   
PriceAndBBs('600556')
#tp=getDeatail('http://guba.eastmoney.com/news,600556,579136853.html') 
#t2,p=getSubDeatail('http://guba.eastmoney.com/news,600556,578926525.html')
#datesNum.plot(secondary_y='num',figsize=(12,5)) 这个数据不全，不好意思。我会再出一份