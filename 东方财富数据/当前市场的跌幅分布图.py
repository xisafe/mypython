import tushare as ts
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import time
def getTag(percent):
    if percent>=7:
        return 7
    elif (percent>=6 and percent<7):
        return 6
    elif (percent>=5 and percent<6):
        return 5
    elif (percent>=4 and percent<5):
        return 4
    elif (percent>=3 and percent<4):
        return 3
    elif (percent>=2 and percent<3):
        return 2
    elif (percent>=1 and percent<2):
        return 1
    elif (percent>=0 and percent<1):
        return 0
    elif (percent>=-1 and percent<0):
        return -1
    elif (percent>=-2 and percent<-1):
        return -2
    elif (percent>=-3 and percent<-2):
        return -3
    elif (percent>=-4 and percent<-3):
        return -4
    elif (percent>=-5 and percent<-4):
        return -5
    elif (percent>=-6 and percent<-5):
        return -6
    else :
        return -7
def getStat():
    nowdata=ts.get_today_all()
    nowdata['tag']=nowdata['changepercent'].apply(lambda x:getTag(x))
    tj=pd.DataFrame(nowdata.groupby('tag')['tag'].count())
    tj_data=pd.DataFrame([-7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7],columns=['tag'])
    tj_data.index=['un6','un5','un4','un3','un2','un1','un0','up0','up1','up2','up3','up4','up5','up6','up7']
    tj['td_count']=tj['tag'].cumsum()
    tj.columns=['tagnum','td_count']
    tj_data=tj_data.merge(tj,how='left',left_on='tag',right_index=True)
    tj=ts.get_index()
    tj_data['percent']=tj_data['tagnum'].cumsum()*1.0/tj_data['tagnum'].sum()
    #plt.rcParams['axes.unicode_minus']=True
    tp=tj[tj['name'].apply(lambda x: x in['上证指数','深证成指','创业板指'])][['name','change','close']]
    plt.figure()
    tj_data['tagnum'].plot(kind='bar',figsize=(10,5),grid=True)
    #tj[['tag','percent']].plot(kind='line',secondary_y='percent',figsize=(12,8),xticks=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16])
    tj_data['percent'].plot(color='r',secondary_y=True,style='-o',linewidth=1,grid=True)
    i=0;
    for x,y in tj_data['percent'].iteritems():
        plt.annotate(format(y,'.1%'), xy=(i, y), xytext=(i, y+0.1),
                     arrowprops=dict(arrowstyle='->',connectionstyle='arc3,rad=.1'))
        i=i+1
    plt.annotate('SH : '+format(tp.iloc[0,1],'.2f')+'%  '+format(tp.iloc[0,2],'.2f'), xy=(1, 1), xytext=(0.1, 1))
    plt.annotate('SZ : '+format(tp.iloc[1,1],'.2f')+'%  '+format(tp.iloc[1,2],'.2f'), xy=(1, 1), xytext=(0.1, 0.95))
    plt.annotate('CY : '+format(tp.iloc[2,1],'.2f')+'%  '+format(tp.iloc[2,2],'.2f'), xy=(1, 1), xytext=(0.1, 0.9))
    #print(tj_data)   
    plt.show()
    #plt.close()
    return tj_data
if __name__ == '__main__':
    if 'timedata' not in dir():
        timedata=pd.DataFrame(columns=['dates','times','un4','un3','un2','un1','un0','up0','up1','up2','up3','up4','fall'])
    dates=time.strftime("%Y-%m-%d", time.localtime())
    timedata=timedata[timedata.dates==dates]
    while True:
        nows=time.localtime()
        times=nows.tm_min+100*nows.tm_hour
        if (times>926 and times<1131) or (times>1258 and times<1501):
            print(time.strftime("%Y-%m-%d %H:%M:%S", nows),'获取数据.....')
            tj=getStat()
            timedata.loc[len(timedata)]={'dates':dates,'times':str(times),'un4':tj.iloc[2,3]-0,'un3':tj.iloc[3,3]-tj.iloc[2,3],'un2':tj.iloc[4,3]-tj.iloc[3,3],'un1':tj.iloc[5,3]-tj.iloc[4,3],'un0':tj.iloc[6,3]-tj.iloc[5,3],'up0':tj.iloc[7,3]-tj.iloc[6,3],'up1':tj.iloc[8,3]-tj.iloc[7,3],'up2':tj.iloc[9,3]-tj.iloc[8,3],'up3':tj.iloc[10,3]-tj.iloc[9,3],'up4':1-tj.iloc[10,3],'fall':tj.iloc[6,3]}
            timedata[['times','un2','un1','un0','up0','up1','up2','fall']].plot(kind='line',x='times',figsize=(11,5)) #,secondary_y='fall'
            time.sleep(180)
        elif (times>=1501 or times<800):
            print(time.strftime("%Y-%m-%d %H:%M:%S", nows),'非交易时间')
            break 
        else:
            print(time.strftime("%Y-%m-%d %H:%M:%S", nows),'中午休市中')
            time.sleep(1800)
        