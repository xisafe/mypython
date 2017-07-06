import math
import os
import pandas as pd
import cons as cons
from sqlalchemy import create_engine
mssql106=cons.MSSQL106('dump20170607')
sql1="""
SELECT
	o.OrderId,
    max(h.CreateDate) StatusDate
   into #stdate
FROM
	[order] AS o (nolock)
INNER JOIN OrderHistory h (nolock) on o.orderid=h.orderid and h.CreateDate>'2017-01-01'
where o.OrderStatusId in (14,32,5,20,29,24,7,10,33,23) and o.ArrivedShanghaiDate>'2017-01-01' 
and o.OriginCode='CN' and  o.updatedate<'2017-05-27'
and   (o.LocalProductOffsetTotal+LocalProductTotal)*o.FirstPaymentExchange>150 
group by o.OrderId HAVING max(h.CreateDate)<'2017-05-25'

SELECT
	o.OrderId,
	o.OrderNumber,
    o.OrderStatusId,
    s.OrderStatusName,
    o.OrderDate,
    c.NickName,
    (o.LocalProductOffsetTotal+LocalProductTotal)*o.FirstPaymentExchange Total_RMB,
    c.CustomerName,c.CatalogCode,st.StatusDate
FROM
	[order] AS o (nolock)
left join OrderStatus s on o.OrderStatusId=s.OrderStatusId
INNER join Customer c (nolock) on o.customerid=c.customerid and c.CatalogCode in('SG','MY')
inner join #stdate st on o.OrderId=st.orderid
where o.updatedate<'2017-05-27' and o.OrderStatusId in (14,32,5,20,29,24,7,10,33,23) and o.ArrivedShanghaiDate>'2017-01-01' and o.OriginCode='CN'
and   (o.LocalProductOffsetTotal+LocalProductTotal)*o.FirstPaymentExchange>150 
;
drop table #stdate;
 """
data = pd.read_sql_query(sql1,con= mssql106)
data=data[data.StatusDate<'2017-05-25']
data.to_excel('/users/hua/no_orders.xlsx',index=False)

#sql1="""select  * from [order] as o where orderdate>'2017-06-01' and purchasetype='ezbuy'  and completedate>'2017-01-01' """
#data = pd.read_sql_query(sql1,con= mssql106)
#data.to_excel('/users/hua/orders.xlsx')