import math
import os
import pandas as pd
import cons as cons
from sqlalchemy import create_engine
mssql106=cons.MSSQL106('dump20170706')
sql1="""
select NickName,CustomerName from customer
 """
if 'customer' not in dir():
    customer= pd.read_sql_query(sql1,con= mssql106)
em=pd.read_excel('/users/hua/temp/email.xlsx')
em2=em.merge(customer,how='left',left_on='nickname',right_on='NickName')
#if 'bills' not in dir():
    #bills= pd.read_sql_query(sql2,con= mssql106)
    #bill2=bills.drop_duplicates()
#package.to_excel('/users/hua/package201706.xlsx')
#billgroup=orders.groupby(by='PaymentBillId').agg({'LocalProductTotal':'sum'})
#billgroup=billgroup.merge(bill2,how='left',left_index=True,right_on='PaymentBillId')
#billgroup=billgroup.fillna(0)
#billgroup.columns=['sumProductTotal', 'PaymentBillId', 'yunfei']
#orders=orders.merge(billgroup,how='left',left_on='PaymentBillId',right_on='PaymentBillId')
#orders['ratio']=orders['LocalProductTotal']/orders['sumProductTotal']
#orders['p']=orders['ratio']*orders['yunfei']
#orders['p2']=orders['LocalProductTotal']**orders['yunfei']/orders['sumProductTotal']