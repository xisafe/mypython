
from lxml import etree
import urllib
import jieba
import pandas as pd
import sqlite3

    
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
    
stockcode='600581' #股票代码
#for line in open('E:/360yun/myprog/outwords.txt','rb'):
#    print(line.rstrip().decode('utf-8'))
#userwords=getSeg(stockcode)
fp=open('E:/360yun/myprog/outwords.txt','rb').read().splitlines()
print(fp)