# -*- coding: utf-8 -*-

import urllib2
import urllib
import pandas as pd
weatherHtml = urllib2.urlopen('http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=FD&sty=MTE&mkt=2&code=000748&ps=10000')
#通过urllib模块中的urlopen的方法打开url
weatherHtml2 = weatherHtml.read().replace("(","").replace(")","")
t=eval(weatherHtml2)
FD_list=[]
for i in t:
    tp=i.split(",")
    if len(tp)!=14:
        print '错误长度',len(tp)
    else :
        #tp=map(lambda a:len(a)>0 and a or '0',tp)
        FD_list.append(tp)

#FD.colnums=['stock','shsz','name','融资余额','日期','融券偿还量','融券卖出量','融券余额','融券余量','融资偿还额','融资买入额','融资融券余额','融资余额','融资净买入额']
#fd=pd.DataFrame(columns=['stock','shsz','name','融资余额','日期','融券偿还量','融券卖出量','融券余额','融券余量','融资偿还额','融资买入额','融资融券余额','融资余额','融资净买入额'])
fd=pd.DataFrame(FD_list)
fd.columns=['stock',u'证券市场','name','rz_ye_er',u'dates','rq_chl','rq_mcl','rq_ye','rq_yl','rz_chl','rz_mre','rz_rq_ye','rz_ye','rz_jmre']
#通过read方法获取返回数据
fd['rz_rq_ye']=fd['rz_rq_ye'].astype('float')
fd['rq_ye']=fd['rq_ye'].astype('float')
fd['rz_ye']=fd['rz_ye'].astype('float')
fd['rq_yl']=fd['rq_yl'].astype('float')
fd['price']=fd['rq_ye']/fd['rq_yl']
fd['dates']=pd.to_datetime(fd['dates'])
fd2=fd.sort_values(by=['dates'],ascending=True)
 
#fd2[['rq_ye','rz_ye','rz_rq_ye','price']].plot(kind='line',secondary_y='price')
fd2[['rq_ye' ,'price']].plot(kind='line',secondary_y='price')
