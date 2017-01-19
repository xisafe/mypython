# -*- coding: utf-8 -*-
import urllib
import pandas as pd
import json
import matplotlib.pyplot as plt
def getMarket(stockcode):
    if stockcode.startswith('6'):
        return 1
    else:
        return 2
def getBSType(dtype):
    if(dtype>0) :
        return '买盘'
    elif(dtype<0):
        return '卖盘'
    else:
        return '中性'
def getDealVol(stockcode,vol=300):
    market=getMarket(stockcode)
    urlraw='http://hqdigi2.eastmoney.com/EM_Quote2010NumericApplication/CompatiblePage.aspx?Type=OB&stk={0}{1}&Reference=xml&limit={2}&page={3}'
    url=urlraw.format(stockcode,market,vol,1) #第一页
    content=urllib.request.urlopen(url).read().decode('utf-8').replace("var jsTimeSharingData=","").replace(";","")
    content=content.replace('pages','"pages"').replace('data','"data"')
    jsondata= json.loads(content)
    newdata=[]
    for i in jsondata['data']:
        tp=i.split(',')
        newdata.append(tp)
    start=2
    while start<=jsondata['pages']:
        url=urlraw.format(stockcode,market,vol,start) #第i页
        #print(url)
        content=urllib.request.urlopen(url).read().decode('utf-8').replace("var jsTimeSharingData=","").replace(";","")
        content=content.replace('pages','"pages"').replace('data','"data"')
        jsondata= json.loads(content)
        for i in jsondata['data']:
            tp=i.split(',')
            newdata.append(tp)
        start=start+1
    newdata=pd.DataFrame(newdata)
    newdata.columns=['times','price','vol','dtype']
    newdata['vol']=newdata['vol'].astype(int)*100
    newdata['price']=pd.to_numeric(newdata['price'])#.astype('float64')
    newdata['dtype']=newdata['dtype'].astype('int')
    newdata['type']=newdata['dtype'].apply(lambda x:getBSType(x))
    return newdata
def getBigVol(df):
    dtype=[u'中性',u'买盘',u'卖盘']
    color=['b','r','g']
    df=df.sort_values(by='times')
    df.index=range(df.shape[0])
    gp= df.groupby(['type'])
    sumt=gp.sum()
    print(sumt)
    plt.figure(figsize=(15,4));
    plt.rcParams['font.sans-serif'] = ['Microsoft YaHei'] #用来正常显示中文标签
    plt.rcParams['font.sans-serif'] = ['SimHei'] #用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False #用来正常显示负号
    for i in range(3):   
        #print(dtype[i])print(color[i])
        plt.bar(df[df['type']==dtype[i]].index, df[df['type']==dtype[i]].vol,alpha=0.7,color=color[i])
    plt.grid(True)
    plt.title(u"大单设置为：")
    #plt.xticks(range(df.shape[0]),range(df.shape[0]))
    plt.margins(0)
    plt.show()
    return df,sumt   #    
stockcode='000066'
vol=300
op=getDealVol(stockcode,vol)
getBigVol(op)