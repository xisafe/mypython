#encoding:utf-8
#from urllib.request import urlopen, Request
from urllib2 import urlopen, Request,StringIO
import pandas as pd
import string as str
#SINA_DD = '%s%s/quotes_service/view/%s?symbol=%s&num=60&page=1&sort=ticktime&asc=0&volume=%s&amount=0&type=0&day=%s
base_url='http://stock.gtimg.cn/data/index.php?appn=detail&action=download&c=sh601857&d=20160510'
re = Request('http://stock.gtimg.cn/data/index.php?appn=detail&action=download&c=sz000912&d=20160512')
lines = urlopen(re, timeout=10).read()
lines = lines.decode('GBK')
#df =pd.read_csv(lines) #df=pd.read_csv(StringIO(lines),sep="\\t") # skiprows=[0]
str=lines.split('\n')
detail_data=[]
#print lines
for tp in str:
    detail_data.append(tp.split('\t'))
mydata=pd.DataFrame(detail_data[1:len(detail_data)+1],columns=['otime','price','change','vol','amount','dtype'])
#mydata.to_excel('d:\mydata.xls',sheet_name='sheet1',)
print mydata#.groupby(['dtype']).sum()