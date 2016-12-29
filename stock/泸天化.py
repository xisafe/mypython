#encoding:utf-8
import tushare as ts
ts.set_token('c7815895aa61e903ffb4e261cdb6fb273942fb3a5edab3842c09cbda6c7e440c')
import  pandas as pd
#import Matplotlib as p
df = ts.get_sina_dd('000912', date='2016-08-01',vol=100) #默认400手
#df = ts.get_sina_dd('600848', date='2015-12-24', vol=500)  #指定大于等于500手的数据
#print df.type;
gp= df.groupby(['type'])
sumt=gp.sum()
df2 = ts.get_realtime_quotes('000912')
#print df.groupby(['type','name']).transform('mean');

print df
print sumt