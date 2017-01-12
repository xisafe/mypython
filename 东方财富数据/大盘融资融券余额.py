# -*- coding: utf-8 -*-
import tushare as ts
import urllib
import pandas as pd
url='http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=FD&sty=SHSZHSSUM&p=1&ps=1000'
rq = urllib.request.urlopen(url) #通过urllib模块中的urlopen的方法打开url #print(weatherHtml2.decode('utf-8')) #.decode('utf-8')
t=eval(rq.read())
FD_list=[]
for i in t:
    tp=i.split(",")
    if len(tp)!=13:
        print ('错误长度',len(tp))
    else :
        FD_list.append(tp)
#[日期','上海融资余额','深圳融资余额','沪深融资余额','上海当日融资买入额','深圳当日融资买入额','沪深当日融资买入额','上海融券余额','深圳融券余额','沪深融券余额','上海融资融券余额','深圳融资融券余额','沪深融资融券余额'])
fd=pd.DataFrame(FD_list)
for i in range(fd.shape[1]):
    if i>0 :
        fd[i]=fd[i].astype('float')/100000000 #转化为亿元
fd.columns=['日期',u'上海融资余额',u'深圳融资余额','沪深融资余额','上海当日融资买入额','深圳当日融资买入额','沪深当日融资买入额','上海融券余额','深圳融券余额','沪深融券余额','上海融资融券余额','深圳融资融券余额','沪深融资融券余额']
###通过read方法获取返回数据
fd.index=fd['日期']
del fd['日期']
#fd=fd.sort_values(by=['dates'],ascending=True)
fd[[u'上海融资余额',u'深圳融资余额',u'沪深融资余额']].plot(kind='line',secondary_y='price',figsize=(18,6),grid=True)
prices=ts.get_k_data(code='sh',start='2013-01-01')
#p=prices[['close','p_change']]
prices=prices.reindex(index=prices['date'],columns=['close'])