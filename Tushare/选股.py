import tushare as ts
stocklist=ts.get_stock_basics()
#p=list(stocklist['industry'].unique()) 行业
quan=stocklist[(stocklist.industry=='环境保护')]
#profit=ts.get_profit_data(2016,3) #盈利能力
#growth=ts.get_growth_data(2016,3) #增长能力
#获取2016年第3季度的现金流量数据
#ts.get_cashflow_data(2016,3) #现金流
#split = ts.profit_data(year=2016)
#获取2016年报的业绩预告数据
#forecast=ts.forecast_data(2016,4)
#report=ts.get_report_data(2016,3)#财务报表
price=ts.get_today_all()