import xlwings as xw
import pandas as pd
import os
from sqlalchemy import create_engine
workdir=os.path.dirname(os.path.realpath(__file__)) #os.getcwd()
def world():
    wb = xw.Book.caller()
    wb.sheets[0].range('A2').value = workdir

def getData(bdate='2017-04-01',edate='2017-05-30'):
    sql="select top 10000 orderid,productname,purchasetype,orderdate,qty,unitprice from orders where orderdate between '{0}' and '{1}'".format(bdate,edate)
    engine= create_engine('mssql+pymssql://publicezbuy:Yangqiang100%@192.168.199.106:1433/ezfinance',echo =False)
    data2 = pd.read_sql_query(sql,con= engine)
    data2.to_excel(workdir+'/out.xlsx',index=False)
    outfile=xw.Book(workdir+'/out.xlsx')
    
 