# coding=utf-8
from lxml import etree
import urllib
import jieba
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
#from wordcloud import WordCloud
import datetime
import tushare as ts
import math
#url='http://guba.eastmoney.com'


def isValidUrl(url='http://www.baidu.com'):#由于错误网页被跳转所以判断不准确
    try :
        rep=urllib.request.urlopen(url,timeout=10)
        print("正常网页")
        return rep.getcode()
    except:
        print("异常")
        return 0
def getDeatail(url="http://guba.eastmoney.com/news,000748,548240785.html"):
    ReData=pd.DataFrame()
    print (url+'读取')
    try:
        rp=urllib.request.urlopen(url,timeout=10)
        html=rp.read()
    except :
        print(url+'失效')
        return pd.DataFrame()
    lists=[]
    dates=[]
    times=[]
    contents=[]
    try:
        page = etree.HTML(html.lower().decode('utf-8'))
    except :
        print(url+'非股吧论坛网页')
        return ReData 
    dt = page.xpath(u"//div[@id='zwconttbn']/strong/node()")
    if len(dt)<1:  #页面不存在情况返回空值
         return ReData
    for h  in dt:  #贴主 信息
        try:
            lists.append((h.text,h.attrib['href']))
        except:
            lists.append((h.text,''))
            pass
    dt = page.xpath(u"//div[@class='zwfbtime']") #帖主时发帖间
    times.append(dt[0].text.split(' ')[2])#times.append(dt[0].text.split(' ')[2])
    dates.append(dt[0].text.split(' ')[1])
    dt = page.xpath(u"//div[@id='zwconbody']/div|//div[@class='zwcontentmain']/div/div/div") #帖主时发帖内容
    if len(dt)>0:    
        contents.append(dt[0].text.strip())
    else :
        contents.append("")
    dt = page.xpath(u"//div[@class='zwlitxt']/div/span[@class='zwnick']/node()")
    for h  in dt:
        try:
            lists.append((h.text,h.attrib['href']))
        except:
            lists.append((h.text,''))
            pass    
    ReData=pd.DataFrame(lists)
    dt = page.xpath(u"//div[@class='zwlitxt']/div[@class='zwlitime']")
    for h  in dt:
        times.append(h.text.split(' ')[3])
        dates.append(h.text.split(' ')[1]) #h.text.lstrip(u"发表于 ")
    dt = page.xpath(u"//div[@class='zwlitxt']/div[@class='zwlitext stockcodec']|//div[@class='zwlitxt']/div[@class='zwlitext yasuo stockcodec']")
    for h  in dt:
            contents.append(h.text)
    try:
        ReData['times']=times
        ReData['dates']=dates
        ReData['contents']=contents
    except :
        return pd.DataFrame()
    ReData.columns=['userName','userUrl','times','dates','contents']
    dt = page.xpath(u"//span[@id='newspage']")
    if len(dt)>0:
        suburl=url.replace('.html','_')
        sumpage=int(math.ceil(int(dt[0].attrib['data-page'].split('|')[1])/30.0))
        if sumpage>10:
            sumpage=10
        for x in range(sumpage-1):
            tpurl=suburl+str(x+2)+'.html'
            tp_pd=getSubDeatail(tpurl)
            ReData=pd.concat([ReData,tp_pd])
    return  ReData  
    
def getSubDeatail(url=""):
    ReData=pd.DataFrame()
    print (url+'读取')
    try:
        rp=urllib.request.urlopen(url,timeout=10)
        html=rp.read()
    except :
        print(url+'失效')
        return pd.DataFrame()
    lists=[]
    dates=[]
    times=[]
    contents=[]
    try:
        page = etree.HTML(html.lower().decode('utf-8'))
    except :
        print(url+'非股吧论坛网页')
        return ReData 
    dt = page.xpath(u"//div[@class='zwlitxt']/div/span[@class='zwnick']/node()")
    if len(dt)<1:
         return ReData
    for h  in dt:
        try:
            lists.append((h.text,h.attrib['href']))
        except:
            lists.append((h.text,''))
            pass    
    ReData=pd.DataFrame(lists)
    dt = page.xpath(u"//div[@class='zwlitxt']/div[@class='zwlitime']")
    for h  in dt:
        times.append(h.text.split(' ')[3])
        dates.append(h.text.split(' ')[1]) #h.text.lstrip(u"发表于 ")
    dt = page.xpath(u"//div[@class='zwlitxt']/div[@class='zwlitext stockcodec']|//div[@class='zwlitxt']/div[@class='zwlitext yasuo stockcodec']")
    for h  in dt:
            contents.append(h.text)
    try:
        ReData['times']=times
        ReData['dates']=dates
        ReData['contents']=contents
    except :
        return pd.DataFrame()
    ReData.columns=['userName','userUrl','times','dates','contents']
    return  ReData
    
def scaryData(stockcode='000748',pages=10): #爬取数据存储在sqlite3中
    bbslist=getBBSlist(stockcode,pages) #获取主贴列表
    allcon=pd.DataFrame(columns=('userName','userUrl','times','dates','contents'))
    total=len(bbslist)
    i=0
    for n,url in bbslist:
        i=i+1
        print(u'共计{0}贴，现在第{1}贴,完成{2}%'.format(total,i,round(i*100.0/total,2)))
        tp=getDeatail(url).dropna()
        allcon=pd.concat([allcon,tp])
    print('爬取完毕') #,url
    allcon['dates']=pd.to_datetime(allcon['dates'],format='%Y-%m-%d')
    allcon=allcon[allcon['dates']>'2016-01-01']
    #allcon['times']=pd.to_timedelta(allcon['times'])
    conn = sqlite3.connect("E:/360yun/myprog/TestData.db") #/360yun/myprog
    runner = conn.cursor()
    runner.execute('drop table IF EXISTS  stockBBS{0}'.format(stockcode))
    allcon.to_sql('stockBBS{0}'.format(stockcode),conn,flavor='sqlite')

def getSeg(stockcode): #获取分词的DataFrame 注意修改一些文件路径
    conn = sqlite3.connect("E:/360yun/myprog/TestData.db") #
    scarydata=pd.read_sql('SELECT userName,userUrl,times,dates,contents FROM stockBBS{0}'.format(stockcode),conn)
    afterseg=[]
    stopwords= {}.fromkeys([line.rstrip().decode('utf-8') for line in open('E:/360yun/myprog/outwords.txt','rb')]) #如果确保唯一可以直接用list
    for i,t in scarydata.iterrows() :
        segs = jieba.cut(t[4])
        for seg in segs:
            if len(seg)>1 and seg  not in stopwords.keys():
                    afterseg.append((t[0],seg))
    userwords=pd.DataFrame(afterseg)
    userwords.columns=['user','words']
    return userwords
    
#def wcfigure(wordsdf,path=r'test.jpg'):#画云图图，传入dataframe,保存文件路径和名字 wdcounts.head(2000).itertuples(index=False)
#    wordcloud = WordCloud(font_path='c:\windows\fonts\STCAIYUN.TTF',background_color="white",margin=5, width=1800, height=1000)
#    #必须要加载文字体不然中文乱码  #print segStat.head(100).itertuples(index=False)
#    wordcloud = wordcloud.fit_words(wordsdf.itertuples(index=False))
#    plt.figure(num=None,figsize=(25, 16),  dpi=8,facecolor='w', edgecolor='k') 
#    plt.imshow(wordcloud)
#    plt.axis("off")
#    plt.savefig(path)
#    plt.show()
#    plt.close()
    
def getBBSlist(stockcode='000748',pages=10,parenturl='http://guba.eastmoney.com'): 
    lists=[]
    for i in range(pages):
        url="http://guba.eastmoney.com/list,{0}_{1}.html".format(stockcode,i+1)
        rp=urllib.request.urlopen(url)
        html=rp.read()
        page = etree.HTML(html.lower().decode('utf-8'))
        hrefs = page.xpath(u"//div[@id='articlelistnew']/div/span[@class='l3']/a")
        #hrefs = page.xpath(u"//div[@id='articlelistnew']/div/span[@class='l3']/a|//span[@class='l6']")
        for h  in hrefs:
            lists.append((h.text,parenturl+h.attrib['href']))
    return lists
def getBaduser(stockcode=''): #庄托信号
    conn = sqlite3.connect("E:/360yun/myprog/TestData.db") #
    badsql="""select h.userName,h.userUrl,h.cmNum,h.cmDays,k.totaldays,
    round(h.cmNum*1.0/k.totaldays,2) day_avg_cmNum,
    round(h.cmDays*1.0/k.totaldays,2) rate_cmDays,
    round(h.cmNum*1.0/h.cmDays,2) cmday_avg_cmNum
    from(
    SELECT userName ,userUrl,count(1) cmNum,
    count(DISTINCT dates)  cmDays FROM stockBBS{0}
    where userUrl>''
    GROUP BY userName,userUrl HAVING count(1)>3 ) h 
    left join
    (SELECT julianday(max(dates))-julianday(min(dates)) totaldays from 
     (SELECT dates from stockBBS{0} GROUP BY dates
      HAVING count(1)>(SELECT 0.4*count(1)/count(DISTINCT dates) num from stockBBS{0}))) k
    where round(h.cmNum*1.0/k.totaldays,2)>0.3 and round(h.cmDays*1.0/k.totaldays,2) >0.1 and round(h.cmNum*1.0/h.cmDays,2)>2
    ORDER BY h.cmNum desc;""".format(stockcode)
    badusers=pd.read_sql(badsql,conn)
    badusers.columns=[u'评论人',u'评论人链接',u'总评论数',u'参与评论天数',u'总天数',u'日均评论数',u'评论参与度',u'参与日日均评论数']
    return badusers
def runAgain(stockcode='',pages=10,runFource=0): #是否重新跑数 一天之内不在重新跑数
    conn = sqlite3.connect("E:/360yun/myprog/TestData.db") #
    #badsql="""SELECT date(max(dates),'start of day','-1 day') maxdate FROM stockBBS{0}""".format(stockcode)
    exitssql="""SELECT rootpage FROM sqlite_master where type='table' and tbl_name='stockBBS{0}'""".format(stockcode)
    cu = conn.cursor()
    cu.execute(exitssql)
    if len(cu.fetchall())>0:
        badsql="""SELECT  max(dates) maxdate FROM stockBBS{0}""".format(stockcode)
        maxdate=pd.read_sql(badsql,conn)
        maxdate['maxdate']=pd.to_datetime(maxdate['maxdate'])
    else :
        runFource=1
    if  runFource>0:
        scaryData(stockcode,pages) #强制跑数
    elif runFource==0 and (pd.to_datetime(datetime.datetime.now())-maxdate.iloc[0,0]).days>10 :
        scaryData(stockcode,pages) #输入股票代码 页数
def PriceAndBBs(stockcode=''): #评论与股价关系
    conn = sqlite3.connect("E:/360yun/myprog/TestData.db") #
    #badsql="""SELECT date(max(dates),'start of day','-1 day') maxdate FROM stockBBS{0}""".format(stockcode)
    ssql="""SELECT substr(dates,0,11) dates,count(1) num FROM stockBBS{0} GROUP BY substr(dates,0,11)""".format(stockcode)
    datesNum=pd.read_sql_query(ssql,conn,index_col='dates')
    price=ts.get_hist_data(stockcode,datesNum.index.min(),datesNum.index.max())
    rs=price.join(datesNum)[['close','num']].fillna(0) #,'price_change'
    rs=rs.sort_index()
    rs.columns=[u'price',u'bbsNum']
    rs.plot(secondary_y=u'bbsNum',figsize=(12,5))
    return rs
            
stockcode='600556' #股票代码
pages=120  #页码
runFource=0 # 强制重新跑数 0不强制，大于0强制
runAgain(stockcode,pages,runFource)
price=PriceAndBBs(stockcode)
badusers=getBaduser(stockcode)
userwords=getSeg(stockcode)
userwords2=userwords #[userwords['user']==u'沪11212006332583']
wdcounts=userwords2.groupby('words').count().sort_values(by=['user'],ascending=False).reset_index().head(6000)
#wcfigure(wdcounts.head(2000),path="{0}.png".format(stockcode))
#
