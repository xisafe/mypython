# -*- coding: utf-8 -*-
import urllib
import pandas as pd
def getMarket(stockcode):
    if stockcode.startswith('6'):
        return 1
    else:
        return 2
def getRzRq(stockcode='000066',listSize=1000):
    market=getMarket(stockcode)
    urlraw='http://ff.eastmoney.com/EM_CapitalFlowInterface/api/js?id={0}{1}&type=hff&rtntype=2'
    url=urlraw.format(stockcode,market)
    data=urllib.request.urlopen(url).read()#.replace("(","").replace(")","") 通过read方法获取返回数据   
    tlist=eval(data)
    FD_list=[]
    for i in tlist:
        tp=i.split(",")
        if len(tp)!=24:
            print ('错误长度',len(tp))
        else :
            FD_list.append(tp)
    fd=pd.DataFrame(FD_list)
    col_name=['日期','主力流入额','主力流出额','主力净流入额',u'主力净流入占比','超大单流入额','超大单流出额',
    '超大单净流入额','超大单净流入占比','大单流入额','大单流出额','大单净流入额','大单净流入占比',
    '中单流入额','中单流出额','中单净流入额','中单净流入占比','小单流入额','小单流出额','小单净流入额','小单净流入占比',
     '股价','涨幅','成交额']
    fd.columns=col_name
    fd['股价']=fd['股价'].astype('float')
    tp=['主力流入额','主力流出额','主力净流入额','超大单流入额','超大单流出额','超大单净流入额','大单流入额',
    '大单流出额','大单净流入额','中单流入额','中单流出额','中单净流入额','小单流入额','小单流出额','小单净流入额','成交额']
    for x in tp:
        if '净' in x:
            fd[x]=fd[x].astype('float')
        else:    
            fd[x]=fd[x].astype('float')/10000
    fd[['主力净流入额' ,'股价']].plot(kind='line',secondary_y='股价',figsize=(18,6),grid=True)       
    return fd
data=getRzRq('600866')