#encoding:utf-8
import tushare as ts
ts.set_token('c7815895aa61e903ffb4e261cdb6fb273942fb3a5edab3842c09cbda6c7e440c')
import  pandas as pd
#import Matplotlib as p
df = ts.get_sina_dd('600401', date='2016-05-12',vol=1000) #默认400手
#df = ts.get_sina_dd('600848', date='2015-12-24', vol=500)  #指定大于等于500手的数据
#print df.type;
gp= df.groupby(['type','name'])
sumt=gp.sum()
#df2 = ts.get_realtime_quotes('000912')
#print df.groupby(['type','name']).transform('mean');
#
#df.encode('GBK')
print df
print sumt
#df.to_csv('d:\s.csv')
#df.to_excel('d:\s.xls')
#print df;
#print ts.sh_margin_details(start='2016-01-01', end='2016-04-19', symbol='600649')
#http://stock.gtimg.cn/data/index.php?appn=detail&action=download&c=sh601857&d=20160510