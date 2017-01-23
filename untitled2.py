# -*- coding: utf-8 -*-
import urllib
import pandas as pd
def getMarket(stockcode):
    if stockcode.startswith('6'):
        return 1
    else:
        return 2
def getRzRq(stockcode='000750',listSize=1000):
    market=getMarket(stockcode)
    urlraw='http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=FD&sty=MTE&mkt={0}&code={1}&ps={2}'
    url=urlraw.format(market,stockcode,listSize)
    data=urllib.request.urlopen(url).read() #.replace("(","").replace(")","") 通过read方法获取返回数据   
    tlist=eval(data)
    FD_list=[]
    for i in tlist:
        tp=i.split(",")
        if len(tp)!=14:
            print ('错误长度',len(tp))
        else :
            #tp=map(lambda a:len(a)>0 and a or '0',tp)
            FD_list.append(tp)
     #['stock','shsz','name','融资余额','日期','融券偿还量','融券卖出量','融券余额','融券余量','融资偿还额','融资买入额','融资融券余额','融资余额','融资净买入额'])
    fd=pd.DataFrame(FD_list)
    fd.columns=['stock',u'证券市场','name','rz_ye_er',u'dates','rq_chl','rq_mcl','rq_ye','rq_yl','rz_chl','rz_mre','rz_rq_ye','rz_ye','rz_jmre']
    fd['rz_rq_ye']=fd['rz_rq_ye'].astype('float')/100000000
    fd['rq_ye']=fd['rq_ye'].astype('float')/100000000
    fd['rz_ye']=fd['rz_ye'].astype('float')/100000000
    fd['rq_yl']=fd['rq_yl'].astype('float')/100000000
    fd['price']=fd['rq_ye']/fd['rq_yl']
    fd['dates']=pd.to_datetime(fd['dates'])
    del fd['证券市场'],fd['rz_ye_er']
    fd=fd.sort_values(by=['dates'],ascending=True)
    fd[['rq_ye' ,'price']].plot(kind='line',secondary_y='price',figsize=(18,6),grid=True)
    return fd
data=getRzRq('000750')    