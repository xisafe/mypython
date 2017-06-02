#encoding:utf-8
import tushare as ts
import time
import matplotlib.pyplot as plt
#default_encoding = 'utf-8'
#if sys.getdefaultencoding() != default_encoding:
#    reload(sys)
#    sys.setdefaultencoding(default_encoding)
def getBigVol(stockCode='000748',vols=400):
    today=time.strftime('%Y-%m-%d')#.decode('utf-8')
    dtype=[u'中性盘',u'买盘',u'卖盘']
    color=['b','r','g']
    df = ts.get_sina_dd(stockCode, date=today,vol=vols) #默认400手
    df=df.sort_values(by='time')
    df['amount']=df.volume*df.price
    df.index=range(df.shape[0])
    gp= df.groupby(['type'])
    sumt=gp.sum()
    sumt['avgPrice']=sumt.amount/sumt.volume
    print(sumt)
    plt.figure(figsize=(15,4));
#    plt.rcParams['font.sans-serif'] = ['Microsoft YaHei'] #用来正常显示中文标签
#    plt.rcParams['font.sans-serif'] = ['SimHei'] #用来正常显示中文标签
#    plt.rcParams['axes.unicode_minus'] = False #用来正常显示负号
    pdf=df.tail(100)
    for i in range(3):     
        plt.bar(pdf[pdf['type']==dtype[i]].index, pdf[pdf['type']==dtype[i]].volume,alpha=0.7,color=color[i])
    plt.grid(True)
    plt.title(stockCode+u"大单设置为："+str(vols))
    #plt.xticks(range(df.shape[0]),range(df.shape[0]))
    plt.margins(0)
    plt.show()
    return df,sumt   #
    
if __name__ == '__main__':
    detail,groupSum=getBigVol('000717',0)
    print(time.strftime('%Y-%m-%d %H:%M:%S')+"上次大单成交时间"+detail.iloc[-1,2]+"  价格："+str(detail.iloc[-1,3])+"  "+detail.iloc[-1,6]+str(detail.iloc[-1,4]))
    print(detail.iloc[-10:-1,2:7])
#    while True:
#        detail,groupSum=getBigVol('000066',100)
#        print(time.strftime('%Y-%m-%d %H:%M:%S')+"上次大单成交时间"+detail.iloc[-1,2]+"  价格："+str(detail.iloc[-1,3])+"  "+detail.iloc[-1,6]+str(detail.iloc[-1,4]))
#        print(detail.iloc[-10:-1,2:7])
#        time.sleep(60)
 