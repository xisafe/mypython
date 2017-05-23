import tushare as ts

stocklist=ts.get_stock_basics()

quan=stocklist[(stocklist.industry=='环保')]