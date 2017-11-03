import math
import os
import pandas as pd
from sqlalchemy import create_engine
# 遍历指定目录，显示目录下的所有文件名
def eachFile(filepath):
    checked=pd.DataFrame(columns=['payment_number','last_total'])
    pathDir =  os.listdir(filepath)
    for allDir in pathDir:
        child = os.path.join('%s/%s' % (filepath, allDir))
        if ".xls" in(child):
            print(child) # .decode('gbk')是解决中文显示乱码问题
            data=pd.read_excel(child)
            data.columns=data.columns.str.strip()
            checked=pd.concat([checked,data[['交易号','总额']]])
    checked=checked.drop_duplicates()
    checked.to_csv('/users/hua/checked.csv')        
    return checked

if __name__ == '__main__':
    filePath='/users/hua/temp/'
    data=pd.read_csv('/users/hua/noname07.csv')
    #data['OrderId']=data['OrderId'].astype('int')
    #data['delivery_month']=data['VerifyProductPayDate'].apply(lambda x: str(x)[0:7].replace('/',''))
    months=list(pd.unique(data['catalog_code']))
    #data['ArrivedShanghaiDate']=pd.to_datetime(data['ArrivedShanghaiDate']) vendor_name
    pk=data.groupby('package_number').agg({'order_id':'nunique','vendor_name':'nunique','入库时间':'max'})
    pk.columns=['订单数','卖家数','最后订单入库日期']
    data=data.merge(pk,how='left',left_on='package_number',right_index=True)
    for m in months:
        tp=data[(data['catalog_code']==m)]
        xname=filePath+m+'.xlsx'
        writer = pd.ExcelWriter(xname, engine='xlsxwriter',options={'strings_to_urls': False})
        print(xname)
        tp.to_excel(writer,index=False)
        writer.close()
    #checked.columns=['payment_number','last_total']
    #data=pd.read_excel("/users/hua/documents/jiehui2015.xlsx")
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

 