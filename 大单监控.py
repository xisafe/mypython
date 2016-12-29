#encoding:utf-8
import tushare as ts
import time
import matplotlib.pyplot as plt
import sys
default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)
def getBigVol(stockCode='000748',vols=400):
    today=time.strftime('%Y-%m-%d').decode('utf-8')
    dtype=[u'中性盘',u'买盘',u'卖盘']
    color=['b','r','g']
    df = ts.get_sina_dd(stockCode, date=today,vol=vols) #默认400手
    df=df.sort_values(by='time')
    df.index=range(df.shape[0])
    gp= df.groupby(['type'])
    sumt=gp.sum()
    print sumt
    plt.figure(figsize=(15,4));
    #plt.rcParams['font.sans-serif'] = ['Microsoft YaHei'] #用来正常显示中文标签
    plt.rcParams['font.sans-serif'] = ['SimHei'] #用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False #用来正常显示负号
    for i in xrange(3):     
        plt.bar(df[df['type']==dtype[i]].index, df[df['type']==dtype[i]].volume,alpha=0.7,color=color[i])
    plt.grid(True)   
    plt.title(stockCode+u"大单设置为："+str(vols))
    #plt.xticks(range(df.shape[0]),range(df.shape[0]))
    plt.margins(0)
    plt.show()
    return df,sumt   #
    
if __name__ == '__main__':
    detail,groupSum=getBigVol('600339',200)
#    while True:
#        getBigVol()
#        print time.strftime('%Y-%m-%d %H:%M:%S').decode('utf-8')
#        time.sleep(50)
 