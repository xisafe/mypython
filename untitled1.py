import tushare as ts

stocklist=ts.get_stock_basics()
del stocklist['pb']
#today=ts.get_today_all()   
tp=today.set_index('code')

del tp['name'],tp['per']
stocklist=stocklist.join(tp)
stocklist['esp_per']=stocklist['esp']/stocklist['trade']
quan=stocklist[(stocklist.industry=='证券')]
quan[quan['trade']<0.1]['trade']=quan[quan['trade']<0.1]['settlement'] 
         
#(r'.*?语音CDMA.*')使用正则表达式进行模糊匹配,*匹配0或无限次,?匹配0或1次
