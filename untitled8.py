import tushare as ts
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
nowdata=ts.get_today_all()
def getTag(percent):
    if percent>=8:
        return 8
    elif (percent>7 and percent<8):
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
    elif (percent>=-7 and percent<-6):
        return -7
    else :
        return -8
nowdata['tag']=nowdata['changepercent'].apply(lambda x:getTag(x))
tj=pd.DataFrame(nowdata.groupby('tag')['tag'].count())
tj['td_count']=tj['tag'].cumsum()
tj['labels']=tj.index
tj=tj.set_index(np.arange(len(tj)))
tj['percent']=tj['tag'].cumsum()*100.0/tj['tag'].sum()
xlabel=pd.DataFrame(['un7','un6','un5','un4','un3','un2','un1','un0','up0','up1','up2','up3','up4','up5','up6','up7','up8'])
#plt.rcParams['axes.unicode_minus']=True
#plt.figure()
tj['tag'].plot(x=tj.index,y='tag',kind='bar',figsize=(12,8))
#tj[['tag','percent']].plot(kind='line',secondary_y='percent',figsize=(12,8),xticks=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16])
tj['percent'].plot(x=tj.index,y='percent',color='r',secondary_y=True,style='-o',linewidth=1)
#plt.xticks(xlabel)
#plt.show()