# coding=utf-8
import os
import urllib2
import jieba
from random import randint
import pandas as pd
import time
import json
import math
import types
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import numpy
#import sys
#reload(sys)
#sys.setdefaultencoding('utf-8')
def getItemId(url):
     uid=url[url.find('id=')+3:url.find('id=')+18]
     if uid.find('&')>0:
         itemid=uid[0:uid.find('&')]
     else :
         itemid=uid
     return itemid
def getRedf(url): #将list数据转换
    relist=[]
    print  url
    try :
        content=json.loads(urllib2.urlopen(url).read().replace('"rateDetail":','').replace('<b>','').replace('</b>',''),'gbk')  #gb18030比gbk含更多字符
    except :
        print url,'json解析错误'
        return pd.DataFrame(),99
    if content.has_key(u'rateList'):
        for i in xrange(len(content[u'rateList'])):
            if type(content['rateList'][i]['appendComment']) is types.DictType:
                 relist.append((content[u'rateList'][i]['displayUserNick'],content[u'rateList'][i]['rateContent'],content[u'rateList'][i]['rateDate'] \
                  ,content[u'rateList'][i][u'appendComment'][u'commentTime'],content[u'rateList'][i][u'appendComment'][u'content']))
            else :
                 relist.append((content[u'rateList'][i]['displayUserNick'],content[u'rateList'][i]['rateContent'],content[u'rateList'][i]['rateDate'],u'',numpy.nan))
        return pd.DataFrame(relist),content['paginator']['lastPage'] #最大页数
    else:
        time.sleep(5)
        print '等待5毫秒：',url
        return getRedf(url)

def getDeatail(itemid='534388747823'):
    #mydf=pd.DataFrame(columns=('user','contents','dates'))
    baseurl="https://rate.tmall.com/list_detail_rate.htm?itemId={0}&sellerId={1}&order=1&currentPage={2}"
    url=baseurl.format(itemid,randint(100000000,700000000),1)
    mydf,pages=getRedf(url)
    #mydf=pd.concat(mydf,first)
    #pages=totals/20+1;
    p=2
    while (p<= pages):
        url=baseurl.format(itemid,randint(100000000,700000000),p)
        print '正在读取第{0}页'.format(p)
        p=p+1
        tpdf,n=getRedf(url)
        mydf=pd.concat([mydf,tpdf])
    mydf.columns=['user','contents','datetime','appendtime','appendcontents']
    return mydf

#scaryData('000748',4) #输入股票代码 页数
def getSeg(scarydata): #获取分词的DataFrame 注意修改一些文件路径
    afterseg=[]
    mywords=[u'什么问题',u'材料差',u'有问题',u'现问题',u'质量差',u'质量好',u'不满意',u'跑不了',u'卖家好',u'服务差',u'服务好']
    for u in mywords:
        jieba.add_word(u)
    stopwords= {}.fromkeys([line.rstrip().decode('utf-8') for line in open('E:/360yun/myprog/outwords.txt')]) #如果确保唯一可以直接用list
    for i,t in scarydata.iterrows() :
        tp=t[0].replace('<b>','').replace('</b>','')
        segs = jieba.cut(tp)
        for seg in segs:
            if len(seg)>1 and seg  not in stopwords.keys():
                    afterseg.append((tp,seg))
    userwords=pd.DataFrame(afterseg)
    userwords.columns=['user','words']
    return userwords
def wcfigure(wordsdf,path=r'test.jpg'):#画云图图，传入dataframe,保存文件路径和名字 wdcounts.head(2000).itertuples(index=False)
    wordcloud = WordCloud(font_path='c:\windows\fonts\STCAIYUN.TTF',background_color="white",margin=5, width=1800, height=1000)
    #必须要加载文字体不然中文乱码  #print segStat.head(100).itertuples(index=False)
    wordcloud = wordcloud.fit_words(wordsdf.itertuples(index=False))
    plt.figure(num=None,figsize=(25, 16),  dpi=8,facecolor='w', edgecolor='k')
    plt.imshow(wordcloud)
    plt.axis("off")
    plt.savefig(path)
    plt.show()
    plt.close()
def refind((x,y)):
    if x>15000 or x<20:
        return False
    else:
        return y
#url='https://detail.tmall.com/item.htm?spm=a230r.1.14.22.CPs6lj&id=534388747823&ns=1&abbucket=10'
url='https://item.taobao.com/item.htm?spm=a1z0d.6639537.1997196601.4.NLuedO&id=527262264900'
#url='https://detail.tmall.com/item.htm?id=532591496697&spm=a1z0k.7385993.1997994373.d4919385.CRvKEJ&_u=t2dmg8j26111'
itemid=getItemId(url)

if os.path.exists(itemid):  #当前路径shous=os.getcwd()
    mydata=pd.read_csv(itemid)
else:
    mydata=getDeatail(itemid)
    mydata.to_csv(itemid,encoding='utf-8',index=False)
mydata['hours']=mydata['datetime'].str[11:13]
mydata['dates']=mydata['datetime'].str[0:10]
mydata['datetime']=pd.to_datetime(mydata['datetime'])
mydata['appendtime']=pd.to_datetime(mydata['appendtime'])
mydata['difftime']=(mydata['appendtime']-mydata['datetime']).apply(lambda x:pd.isnull(x) and 200 or x.total_seconds()) #追评相差天数
mydata['wordsnum']=((mydata['contents'].str.len()+mydata['appendcontents'].str.len())/3).apply(lambda x : math.ceil(x))  #apply对每个元素操作
mydata['specialwords']= mydata['contents'].str.count('<')
tp2=mydata.groupby(by=[u'user'])[u'user'].agg({u"计数":numpy.size})\
             .sort_values(by=[u"计数"],ascending=False).reset_index()
baduser=list(tp2[tp2[u'计数']>4]['user'])
badlist=((mydata['difftime']<100000)& (mydata['wordsnum']<100)) #| (mydata['user'].isin(baduser))
mydata['badflag']=((mydata['difftime']>=100000) | (mydata['difftime']<60) | badlist) & (~mydata['user'].isin(baduser)) #|(~badlist) #| (~mydata['user'].isin(baduser))
#mydata['badflag']= mydata[['difftime','badflag2']].apply(refind,axis=1)
#mydata['badflag2']=badlist# mydata[['difftime','badflag2']].apply(refind,axis=1)
#del mydata['badflag2']
tp=mydata[mydata['contents']>''].groupby(by=[u'contents'])[u'contents'].agg({u"计数":numpy.size})\
             .sort_values(by=[u"计数"],ascending=False).reset_index()
groupby_date=mydata.groupby(by=[u'dates'])[u'dates'].agg({u"评论数":numpy.size})
groupby_hour=mydata.groupby(by=[u'hours'])[u'hours'].agg({u"评论数":numpy.size})
groupby_date=mydata.groupby(by=[u'dates'])[u'appendcontents'].agg({u'总评论数':numpy.size,u'追评数':'count'})
groupby_date[u'追评率']=groupby_date[u'追评数']*1.0/groupby_date[u'总评论数']
grouby_badflag=mydata.groupby(u'badflag')[u'badflag'].agg({u"评论数":numpy.size})
dup=sum(tp2[tp2[u'计数']>4][u'计数'])*1.00/mydata.shape[0] #用户刷单数
#dup2=sum(tp[(tp[u'计数']>=2)&(tp['contents']<>'15天内买家未作出评价')][u'计数'])*1.00/mydata.shape[0] #按评论内容刷单数
badRate=float(grouby_badflag[grouby_badflag.index==True][u'评论数'])/mydata.shape[0]
print '重复评论用户占比： {0}%; 疑似刷单数占比：{1}%'.format(format(dup*100,'.2f'),format(badRate*100,'.2f'))
groupby_date.plot(kind='line',color=['g','r','b'],title=u'按日汇总评论',figsize=(10,4))
#groupby_hour.plot(kind='line',color='g',title=u'按24时汇总评论')
bad=mydata[mydata['badflag']==True]['contents'].dropna().append(mydata[mydata['badflag']==True]['appendcontents'].dropna())
#nodup=tp[tp[u'计数']==1]
#bad=pd.DataFrame(bad)
#userwords=getSeg(bad)
#wdcounts=userwords.groupby('words').count().sort_values(by=['user'],ascending=False).reset_index() #.head(6000)
#wcfigure(wdcounts.head(2000),'{0}bad.jpg'.format(itemid))
#good=mydata[mydata['badflag']==False]['contents'].dropna().append(mydata[mydata['badflag']==False]['appendcontents'].dropna())
##del good['user']
#good=pd.DataFrame(good)
#good_userwords=getSeg(good)
#good_wdcounts=good_userwords.groupby('words').count().sort_values(by=['user'],ascending=False).reset_index() #.head(6000)
#wcfigure(good_wdcounts.head(2000),'{0}good.jpg'.format(itemid))
