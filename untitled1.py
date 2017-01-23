import tushare as ts

stocklist=ts.get_stock_basics()
quan=stocklist[(stocklist.industry=='证券')]
#(r'.*?语音CDMA.*')使用正则表达式进行模糊匹配,*匹配0或无限次,?匹配0或1次
