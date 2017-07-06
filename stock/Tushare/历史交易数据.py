import tushare as ts
import sqlite3
import pandas as pd
import datetime
import math
dbname='stocks'
dbpath='E:\myprog\TestData.db'
#dbpath="/Users/hua/documents/TestData.db"
if 'stocks' not in dir():
    stocks=ts.get_stock_basics()  
def getTag(x):
    tag=math.floor(x)
    if tag>=8 :
        return 8
    elif tag<=-8:
        return -8
    else:
        return tag
def tableCheck():
    curs= conn.cursor()
    sql="SELECT name num FROM sqlite_master WHERE type='table' AND name='stocks'"
    curs.execute(sql)
    isexists=curs.fetchall()
    if len(isexists)>0:
        curs.close()
    else:
        sql="""CREATE TABLE stocks (
         date TEXT,
         open REAL,
         high REAL,
         close REAL,
         low REAL,
         volume REAL,
         price_change REAL,
         p_change REAL,
         ma5 REAL,
         ma10 REAL,
         ma20 REAL,
         v_ma5 REAL,
         v_ma10 REAL,
         v_ma20 REAL,
         turnover REAL,
         code TEXT,
         tag REAL
         )"""
        curs.execute(sql)
        curs.close()

def getLastDate():
    curs= conn.cursor()
    sql="SELECT code,date(ifnull(max(date),'2000-01-01'),'start of day','1 day') maxdate FROM stocks group by code"
    curs.execute(sql)
    dates=curs.fetchall()
    if len(dates)>0:
        return dict(dates)
    else:
        return {'1000':'2000-01-01'}
if __name__=='__main__':
    yes_time =  datetime.datetime.now() + datetime.timedelta(days=-1)
    enddate=yes_time.strftime("%Y-%m-%d")
    conn = sqlite3.connect(dbpath) #
    tableCheck()
    mapdate=getLastDate()
    i=1
    total=len(stocks)
    stlist=list(stocks.index)
    stlist.sort()
    stlist.append('sh')
    stlist.append('sz')
    for code in stlist:
        print("正在获取第",i,"个,共：",total,"-----",code)
        if code in mapdate.keys():
            startdate=mapdate[code]
        else:
            startdate='2001-01-01'
        i=i+1
        if enddate>startdate:
            d=ts.get_hist_data(code,startdate,enddate)
            if d is not None and len(d)>0:
                d['code']=code
                d['tag']=d['p_change'].apply(getTag)
                if 'turnover' not in d.columns:
                    d['turnover']=math.nan
                d[[ "open", "high", "close", "low", "volume", "price_change", "p_change", "ma5","ma10", "ma20", "v_ma5", "v_ma10", "v_ma20", "turnover", "code", "tag"]].to_sql(dbname,conn,flavor='sqlite',if_exists='append')
    conn.close()        