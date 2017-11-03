import math
import os
import pandas as pd
import cons as cons
from sqlalchemy import create_engine
mssql106=cons.MSSQL106('dump20170706')
sql1="""
SELECT
	o.OrderNumber,
	o.SendToSgDate,
	o.ArrivedSingporeDate,
	o.LocalProductTotal,
	o.LocalProductOffsetTotal,
	o.LocalGstTotal,
	o.LocalGstOffset,
    o.WarehouseCode,
    o.OriginCode,
    s.VptNumber,
    t.ShipmentTypeName,
    o.PaymentBillId
FROM
	[order] o (nolock)
inner join Customer c (nolock) on o.CustomerId=c.CustomerId and c.CatalogCode='SG'
INNER JOIN ShipmentType t ON o.ShipmentTypeId = t.ShipmentTypeId
LEFT JOIN PackingNumber pn (nolock) ON o.PackingNumberId = pn.PackingNumberId
LEFT JOIN LogisticsShipping s (nolock) ON pn.LogisticsShippingId = s.LogisticsShippingId
WHERE o.OriginCode in('CN','US') and
	o.SendToSgDate > '2017-05-01'
AND o.SendToSgDate < '2017-07-01'
union all
SELECT
	o.OrderNumber,
	o.SendToSgDate,
	o.ArrivedSingporeDate,
	o.LocalProductTotal,
	o.LocalProductOffsetTotal,
	o.LocalGstTotal,
	o.LocalGstOffset,
    o.WarehouseCode,
    o.OriginCode,
    s.VptNumber,
    t.ShipmentTypeName,
    o.PaymentBillId
FROM
	dumpeznearline20170706.dbo.[order] o (nolock)
inner join Customer c (nolock) on o.CustomerId=c.CustomerId and c.CatalogCode='SG'
INNER JOIN ShipmentType t ON o.ShipmentTypeId = t.ShipmentTypeId
LEFT JOIN PackingNumber pn (nolock) ON o.PackingNumberId = pn.PackingNumberId
LEFT JOIN LogisticsShipping s (nolock) ON pn.LogisticsShippingId = s.LogisticsShippingId
WHERE o.OriginCode in('CN','US') and
	o.SendToSgDate > '2017-05-01'
AND o.SendToSgDate < '2017-07-01'
 """
sql2="""
SELECT
	o.OrderNumber,
	o.SendToSgDate,
	o.ArrivedSingporeDate,
	o.LocalProductTotal,
	o.LocalProductOffsetTotal,
	o.LocalGstTotal,
	o.LocalGstOffset,
    o.WarehouseCode,
    o.OriginCode,
    s.VptNumber,
    t.ShipmentTypeName,pn.packingcode,
    o.PaymentBillId
FROM
	[order] o (nolock)
inner join Customer c (nolock) on o.CustomerId=c.CustomerId and c.CatalogCode='SG'
INNER JOIN ShipmentType t ON o.ShipmentTypeId = t.ShipmentTypeId
LEFT JOIN PackingNumber pn (nolock) ON o.PackingNumberId = pn.PackingNumberId
LEFT JOIN LogisticsShipping s (nolock) ON pn.LogisticsShippingId = s.LogisticsShippingId
WHERE o.OriginCode in('CN','US') and
	o.SendToSgDate > '2017-06-01'
AND o.SendToSgDate < '2017-07-01'
union all
SELECT
	o.OrderNumber,
	o.SendToSgDate,
	o.ArrivedSingporeDate,
	o.LocalProductTotal,
	o.LocalProductOffsetTotal,
	o.LocalGstTotal,
	o.LocalGstOffset,
    o.WarehouseCode,
    o.OriginCode,
    s.VptNumber,
    t.ShipmentTypeName,pn.packingcode,
    o.PaymentBillId
FROM
	dumpeznearline20170706.dbo.[order] o (nolock)
inner join Customer c (nolock) on o.CustomerId=c.CustomerId and c.CatalogCode='SG'
INNER JOIN ShipmentType t ON o.ShipmentTypeId = t.ShipmentTypeId
LEFT JOIN PackingNumber pn (nolock) ON o.PackingNumberId = pn.PackingNumberId
LEFT JOIN LogisticsShipping s (nolock) ON pn.LogisticsShippingId = s.LogisticsShippingId
WHERE o.OriginCode in('CN','US') and
	o.SendToSgDate > '2017-06-01'
AND o.SendToSgDate < '2017-07-01'
""" 
if 'package' not in dir():
    package= pd.read_sql_query(sql2,con= mssql106)
#if 'bills' not in dir():
    #bills= pd.read_sql_query(sql2,con= mssql106)
    #bill2=bills.drop_duplicates()
package.to_excel('/users/hua/orderdata_gst201706.xlsx')
#billgroup=orders.groupby(by='PaymentBillId').agg({'LocalProductTotal':'sum'})
#billgroup=billgroup.merge(bill2,how='left',left_index=True,right_on='PaymentBillId')
#billgroup=billgroup.fillna(0)
#billgroup.columns=['sumProductTotal', 'PaymentBillId', 'yunfei']
#orders=orders.merge(billgroup,how='left',left_on='PaymentBillId',right_on='PaymentBillId')
#orders['ratio']=orders['LocalProductTotal']/orders['sumProductTotal']
#orders['p']=orders['ratio']*orders['yunfei']
#orders['p2']=orders['LocalProductTotal']**orders['yunfei']/orders['sumProductTotal']