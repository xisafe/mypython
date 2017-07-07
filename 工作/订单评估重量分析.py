import math
import numpy as np
import urllib
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
def getHost(url='www.google.cn'):
    if not url.startswith('http'):
        url="http://"+url
    proto, rest = urllib.parse.splittype(url)
    res, rest = urllib.parse.splithost(rest)   
    return res.replace('www.','')
    
if __name__ == '__main__':
    filePath = "/users/hua/documents/temp/prime_orders/"
    data=pd.read_csv('/users/hua/prime_orders.csv')
    #data['delivery_month']=data['arrive_date'].apply(lambda x: str(x)[0:7].replace('-',''))
    #months=list(pd.unique(data['delivery_month']))
    data['hosts']=data['producturl'].apply(getHost)
    del data['productname'],data['webdomain']
    data['max_weight']=data[['weight','volumweight']].max(axis=1)
    data['weight_or_vol']=data['max_weight']==data['weight']
    data.loc[data['chargeweight']==0,'chargeweight']=0.0001
    data['diff_weight']=(data[['weight','volumweight']].max(axis=1)-data['chargeweight']/1.1)/1000
    data['diff_rate']=(data[['weight','volumweight']].max(axis=1)*1.1-data['chargeweight'])*100/data['chargeweight']
    #data.loc[np.isinf(data['diff_rate']),'diff_rate']=400
    #data.hist(by='diff_rate',bins=20,figsize=(10,7))
    #tp=data[np.isinf(data['diff_rate'])].index
'''    
 select 
o.ord_nr OrderNumber,
o.pkg_nr packageNumber,
o.prod_nm productName,o.prod_url_txt ProductUrl,
o.vndr_shop_nm vendrName,
o.qty,
o.trnsn_wrhs_cd WhareHouse,
o.vol_wght volumWeight,
o.wght Weight,
o.ord_charge_wght chargeWeight,
o.est_wght EstWeight,
-- o.shpmt_type_nm,
o.prod_url_dmn_nm webDomain,
o.cust_ctry_cd catalog_code,
o.shpmt_type_nm ShipmentTypeName,
o.ez_dis_prod_catg_lvl_1_nm,
o.ez_dis_prod_catg_lvl_2_nm,
o.ez_dis_prod_catg_lvl_3_nm,
trunc(o.ord_arrive_trnsn_wrhs_ts) arrive_date
 from dw.rz_sls_ord_f o where o.ord_arrive_trnsn_wrhs_ts>='2017-01-01'  and o.prch_type_cd='Prime'

'''          

 