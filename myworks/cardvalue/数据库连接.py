import math
import os
import pandas as pd
import cons as cons
from sqlalchemy import create_engine
mssql106=cons.MSSQL106('dump20170607')
sql1="""select  * from [order] as o where orderdate>'2017-06-01' and purchasetype='ezbuy'  and completedate>'2017-01-01' """
data = pd.read_sql_query(sql1,con= mssql106)
statis=cons.statis()
data2 = pd.read_sql_query('select  * from kpi_department ',con= statis)
purchasecheck=cons.mongo('purchasecheck')
for u in purchasecheck.model.Order.find({"Refund":{"$gt":0}}):
    print(u) #db.a.find( { $where : "this.Refund < this.Total"} )