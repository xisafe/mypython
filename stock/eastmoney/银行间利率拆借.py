import tushare as ts
import pandas as pd
shibor2014 = ts.shibor_data(2014) #取当前年份的数据
shibor2015 = ts.shibor_data(2015)
shibor2016 = ts.shibor_data(2016)
shibor2017 = ts.shibor_data(2017)
shibor=pd.concat([shibor2014[['date','ON']], shibor2015[['date','ON']],shibor2016[['date','ON']], shibor2017[['date','ON']]])