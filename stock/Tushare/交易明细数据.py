import tushare as ts
import pandas as pd
#df = ts.get_tick_data('600581',date='2015-01-09')
#timedelta_range(start='1 days', periods=5, freq='D') timedelta_range(start='1 days', end='2 days', freq='30T')
#pd.period_range('201001','201005',freq='M')
#pd.TimedeltaIndex(['1 days', '1 days, 00:00:05', np.timedelta64(2,'D'), np.timedelta(days=2,seconds=2)])
def getDeatil(start,end,stockcode):
    allData=pd.DataFrame()
    for x in pd.period_range(start,end,freq='D'):
        print(x)
        detail = ts.get_tick_data(stockcode,date=x)
        if detail.shape[0]>40:
            del detail['change']
            detail['dates']=x
            allData=pd.concat([detail,allData])
        else:
            print(str(x)+'日无数据')
    allData=allData.reset_index(drop=True)  
    return allData
if __name__ =='__main__':
    deal2= getDeatil('2017-01-01','2017-02-18','600581') 