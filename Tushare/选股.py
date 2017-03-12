import tushare as ts
stocklist=ts.get_stock_basics()
del stocklist['area'],stocklist['pb']
#p=list(stocklist['industry'].unique()) 行业
quan=stocklist[(stocklist.industry=='环境保护')]
#profit=ts.get_profit_data(2016,3) #盈利能力
#growth=ts.get_growth_data(2016,3) #增长能力
#获取2016年第3季度的现金流量数据
#ts.get_cashflow_data(2016,3) #现金流
#split = ts.profit_data(year=2016)
#获取2016年报的业绩预告数据
if 'forecast' not in dir():
    forecast=ts.forecast_data(2016,4) #获取2016年报的业绩预告数据
    forecast=forecast.set_index(forecast.code)
    forecast=forecast.drop_duplicates(['code'])
    del forecast['code']
gains=forecast[forecast['type'].isin(['预升','预增','预盈'])]
gains=gains.drop_duplicates(['name'])
del gains['name']
#report=ts.get_report_data(2016,3)#财务报表
if 'price' not in dir(): #获取当天的交易价格
    price=ts.get_today_all()
    price=price.set_index(price.code)
    del price['code'],price['name']
stock=stocklist[stocklist['esp']>0]
stock=stock.join(price,rsuffix='_2')
stock['RevPerMoney']=stock['esp']/stock['settlement']
stock=stock[stock['settlement']<12]
stock=stock[stock['totals']<12]
stock=stock.join(forecast,rsuffix='_2')