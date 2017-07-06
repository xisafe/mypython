import pandas as pd
from matplotlib.dates import DateFormatter, WeekdayLocator,DayLocator, MONDAY,date2num
from matplotlib.finance import candlestick_ohlc
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
dbpath='E:\myprog\TestData.db' #数据库文件完整路径
stockcode='600866' # 股票代码
begindate='2017-02-01' #交易开始日期
enddate='2017-06-12'  #交易结束日期
engine= create_engine('sqlite:///{0}'.format(dbpath)) 
# 使用sqlalchemy连接数据库，python可以简单连接sqlite，但是为了方便数据库类型变更使用sqlalchemy，支持多种数据库
stdata = pd.read_sql_query("select * from stocks where code='{0}' and date between '{1}' and '{2}'".format(stockcode,begindate,enddate),con= engine,index_col='date')
stdata.index= pd.to_datetime(stdata.index)
stdata=stdata.sort_index()
stdata['return'] = stdata ['close'] / stdata.close.iloc[0]
stdata['return'].plot(grid=True)
stdata['p_change'].plot(grid=True,figsize=(12,7)).axhline(y=0, color='black', lw=2)
stdata[['close','turnover']].plot(figsize=(22,8),secondary_y='close',grid=True)
close_price = stdata['close']
log_change = np.log(close_price) - np.log(close_price.shift(1))
log_change.plot(grid=True,figsize=(12,7)).axhline(y=0, color='black', lw=2)
small = stdata[['close', 'price_change', 'ma20','volume', 'v_ma20', 'turnover']]
#矩阵散点图
pd.scatter_matrix(small,figsize=(18,12))
small = stdata[['close', 'price_change', 'ma20','volume', 'v_ma20', 'turnover']]
#相关性，相关系数
cov = np.corrcoef(small.T)
plt.figure(figsize=(12,7))
img = plt.matshow(cov,cmap=plt.cm.winter)
#相关性矩阵图
plt.colorbar(img, ticks=[-1,0,1])
plt.show()
#收盘价和Ma20的关系图
stdata[['close','ma20']].plot(secondary_y='ma20',figsize=(12,7), grid=True)
def pandas_candlestick_ohlc(dat, stick = "day", otherseries = None):
    mondays = WeekdayLocator(MONDAY)        # major ticks on the mondays
    alldays = DayLocator()              # minor ticks on the days
    #dayFormatter = DateFormatter('%d')      # e.g., 12
    # Create a new DataFrame which includes OHLC data for each period specified by stick input
    transdat = dat.loc[:,["open", "high", "low", "close"]]
    if (type(stick) == str):
        if stick == "day":
            plotdat = transdat
            stick = 1 # Used for plotting
        elif stick in ["week", "month", "year"]:
            if stick == "week":
                transdat["week"] = pd.to_datetime(transdat.index).map(lambda x: x.isocalendar()[1]) # Identify weeks
            elif stick == "month":
                transdat["month"] = pd.to_datetime(transdat.index).map(lambda x: x.month) # Identify months
            transdat["year"] = pd.to_datetime(transdat.index).map(lambda x: x.isocalendar()[0]) # Identify years
            grouped = transdat.groupby(list(set(["year",stick]))) # Group by year and other appropriate variable
            plotdat = pd.DataFrame({"open": [], "high": [], "low": [], "close": []}) # Create empty data frame containing what will be plotted
            for name, group in grouped:
                plotdat = plotdat.append(pd.DataFrame({"open": group.iloc[0,0],
                                            "high": max(group.high),
                                            "low": min(group.low),
                                            "close": group.iloc[-1,3]},
                                           index = [group.index[0]]))
            if stick == "week": stick = 5
            elif stick == "month": stick = 30
            elif stick == "year": stick = 365
 
    elif (type(stick) == int and stick >= 1):
        transdat["stick"] = [np.floor(i / stick) for i in range(len(transdat.index))]
        grouped = transdat.groupby("stick")
        plotdat = pd.DataFrame({"open": [], "high": [], "low": [], "close": []}) # Create empty data frame containing what will be plotted
        for name, group in grouped:
            plotdat = plotdat.append(pd.DataFrame({"open": group.iloc[0,0],
                                        "high": max(group.High),
                                        "low": min(group.Low),
                                        "close": group.iloc[-1,3]},
                                       index = [group.index[0]]))
 
    else:
        raise ValueError('Valid inputs to argument "stick" include the strings "day", "week", "month", "year", or a positive integer')
    # Set plot parameters, including the axis object ax used for plotting
    fig, ax = plt.subplots()
    fig.subplots_adjust(bottom=0.2)
    if plotdat.index[-1] - plotdat.index[0] < pd.Timedelta('730 days'):
        weekFormatter = DateFormatter('%b %d')  # e.g., Jan 12
        ax.xaxis.set_major_locator(mondays)
        ax.xaxis.set_minor_locator(alldays)
    else:
        weekFormatter = DateFormatter('%b %d, %Y')
    ax.xaxis.set_major_formatter(weekFormatter)
    ax.grid(True)
    # Create the candelstick chart
    candlestick_ohlc(ax, list(zip(list(date2num(plotdat.index.tolist())), plotdat["open"].tolist(), plotdat["high"].tolist(),
                      plotdat["low"].tolist(), plotdat["close"].tolist())),
                      colorup = "red", colordown = "green", width = stick * .4)
    # Plot other series (such as moving averages) as lines
    if otherseries != None:
        if type(otherseries) != list:
            otherseries = [otherseries]
        dat.loc[:,otherseries].plot(ax = ax, lw = 1.3, grid = True)
    ax.xaxis_date()
    ax.autoscale_view()
    fig.set_figheight(6)
    fig.set_figwidth(10)
    plt.setp(plt.gca().get_xticklabels(), rotation=45, horizontalalignment='right')
    plt.show()
pandas_candlestick_ohlc(stdata,stick = "day",otherseries=['ma5','ma20'])
