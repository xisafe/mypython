import math
from matplotlib.ticker import FuncFormatter
import numpy as np
import urllib
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import sys
from matplotlib import gridspec
from sqlalchemy import create_engine
def getHost(url='www.google.cn'):
    if not url.startswith('http'):
        url="http://"+url
    proto, rest = urllib.parse.splittype(url)
    res, rest = urllib.parse.splithost(rest)   
    return res.replace('www.','')
def to_percent(y, position):
    print('raw:',y)
    if y.max()>1:
        y=y/10
    print(y)
    s = str(100 * y)
    if matplotlib.rcParams['text.usetex'] is True:
        return s + r'$\%$'
    else:
        return s + '%' 
def plotDist(X,title='',bins_k=40):
    X=np.array(X)
    print(title)
    formatter = FuncFormatter(to_percent)
    plt.figure(figsize=(10, 6)) 
    gs = gridspec.GridSpec(2, 1, height_ratios=[1,10]) 
    ax0 = plt.subplot(gs[0])
    ax0.boxplot(X,vert=False)
    plt.grid()
    ax1 = plt.subplot(gs[1])
    ax1.set_ylabel('hist distribution')
    t=ax1.hist(X, bins=bins_k, normed=True,color='g')
    #print(t[0])
    plt.gca().yaxis.set_major_formatter(formatter)
    plt.grid()
    ax2 = ax1.twinx()  # this is the important function
    ax2.set_ylabel(' cumulative distribution')
    plt.gca().yaxis.set_major_formatter(formatter)
    acc=ax2.hist(X, bins=bins_k, normed=True,color='red',cumulative=True,histtype='step')
    plt.annotate('Mean : '+format(X.mean(),'.2f')+'', xy=(-90, 1), xytext=(-90, 1))
    plt.annotate(' Std :' + format(X.std(),'.2f')+'', xy=(-90, 1), xytext=(-90, 0.95))
    plt.annotate(' Var :' + format(X.var(),'.2f')+'', xy=(-90, 1), xytext=(-90, 0.90))
    plt.tight_layout()
    plt.show()
    plt.close()
    return acc,t[0]

if __name__ == '__main__':
    filePath = "/users/hua/documents/temp/prime_orders/"
    if 'data' not in dir():
        data=pd.read_csv(r'E:\BaiduYunDownload\prime_orders.csv')
        #data=pd.read_csv('/users/hua/prime_orders.csv')
        del data['productname'],data['webdomain']
        data['hosts']=data['producturl'].apply(getHost)
        data['max_weight']=data[['weight','volumweight']].max(axis=1)
        data['weight_or_vol']=data['max_weight']==data['weight']
        zero_data=data[(data['max_weight']==0)|(data['chargeweight']==0)]
        data['diff_weight']=(data[['weight','volumweight']].max(axis=1)-data['chargeweight']/1.1)/1000
        data['diff_rate']=(data[['weight','volumweight']].max(axis=1)*1.1-data['chargeweight'])*100/data['chargeweight']
        norm_data=data[(data['max_weight']>0)&(data['chargeweight']>0)].copy()#
    #data['delivery_month']=data['arrive_date'].apply(lambda x: str(x)[0:7].replace('-',''))
    #data.loc[data['diff_rate']>500,'diff_rate']=500
    norm_data.loc[norm_data['diff_weight']>4,'diff_weight']=4
    norm_data.loc[norm_data['diff_weight']<-4,'diff_weight']=-5
    #zero_data_st=zero_data.groupby('hosts')['hosts'].count()
    #norm_data.loc[norm_data['diff_rate']>500,'diff_rate']=500
    outlier=norm_data[(norm_data['diff_weight']<-1)|(norm_data['diff_weight']>1)] #离群样本
    #outlier_st=outlier.groupby('hosts')['hosts'].count()
    #plt.figure()
    #plt.title('distributed by all diff_rate')
    #norm_data['diff_weight'].hist(bins=20,figsize=(10,7))
    print("1、volumWeight与Weight同为0或者chargeweight为0 视为无效,已经排除 无效值占比: %.2f%%" % (zero_data.shape[0]*100/data.shape[0]))
    print("2、diff_rate>=500%的占比1.85% diff_rate>=200%占比3.22% diff_rate>100%占比5.42%" )
    print("3、diff_rate>100%占比5.42% 视为离群值 或离群点outlier,大概是4倍标准误差的范围，mean+4*std,也确保95%的样本" )
    plt.show()
    plt.close()
    valid_data=norm_data[(norm_data['diff_weight']<=1)&(norm_data['diff_weight']>=-1)] #正常群体样本
    st,st2=plotDist(valid_data['diff_weight'],title='',bins_k=20)
    print('1、diff_rate<=0% 累计占比60.4% diff_rate<=10% 累计占比88.5% -10%<diff_rate<10% 占比约62%')
    print('2、分布非常对称') 
    x=valid_data[valid_data['weight_or_vol']==True]['diff_weight']
