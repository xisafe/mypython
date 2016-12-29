# coding=utf-8
from lxml import etree
import urllib2
import jieba
from random import randint
import pandas as pd
import StringIO
import json
import sqlite3
import matplotlib.pyplot as plt
from wordcloud import WordCloud
url='https://detail.tmall.com/item.htm?spm=a230r.1.14.22.CPs6lj&id=534388747823&ns=1&abbucket=10'
def getItemId(url=url):
     uid=url[url.find('id=')+3:url.find('id=')+18]
     itemid=uid[0:uid.find('&')]
     return itemid
def getDeatail(itemid='534388747823'):
    url="https://rate.tmall.com/list_detail_rate.htm?itemId={0}&sellerId={1}&currentPage={2}"
    url=url.format(itemid,randint(100000000,700000000),1)
     
    #resp = urllib2.urlopen(req,None,req_timeout)
    j = StringIO. StringIO(urllib2.urlopen(url).read())
    print j.read()
    #content= urllib2.urlopen(url).read()
    #content=requests.get(url).content
    #content=resp.read()
    return j.read()

#scaryData('000748',4) #输入股票代码 页数
def getSeg(): #获取分词的DataFrame 注意修改一些文件路径
    conn = sqlite3.connect("E:/360yun/myprog/TestData.db") #
    scarydata=pd.read_sql('SELECT userName,userUrl,times,dates,contents FROM stockBBS',conn)
    afterseg=[]
    stopwords= {}.fromkeys([line.rstrip().decode('utf-8') for line in open('E:/360yun/myprog/outwords.txt')]) #如果确保唯一可以直接用list
    for i,t in scarydata.iterrows() :
        segs = jieba.cut(t[4])
        for seg in segs:
            if len(seg)>1 and seg  not in stopwords.keys():
                    afterseg.append((t[0],seg))
    userwords=pd.DataFrame(afterseg)
    userwords.columns=['user','words']
    return userwords
def wcfigure(wordsdf,path=r'd:\test.jpg'):#画云图图，传入dataframe,保存文件路径和名字 wdcounts.head(2000).itertuples(index=False)
    wordcloud = WordCloud(font_path='c:\windows\fonts\STCAIYUN.TTF',background_color="white",margin=5, width=1800, height=1000)
    #必须要加载文字体不然中文乱码  #print segStat.head(100).itertuples(index=False)
    wordcloud = wordcloud.fit_words(wordsdf.itertuples(index=False))
    plt.figure(num=None,figsize=(25, 16),  dpi=8,facecolor='w', edgecolor='k') 
    plt.imshow(wordcloud)
    plt.axis("off")
    plt.savefig(path)
    plt.show()
    plt.close()

#ur=getDeatail('534388747823')#'https://rate.tmall.com/list_detail_rate.htm?itemId=534388747823&sellerId=332025450&currentPage=1'
url="https://rate.tmall.com/list_detail_rate.htm?itemId={0}&sellerId={1}&currentPage={2}"
url=url.format('534388747823',randint(100000000,700000000),1)
     
    #resp = urllib2.urlopen(req,None,req_timeout)
j=json.loads(urllib2.urlopen(url).read())

