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
    filePath = "/users/hua/documents/temp/prime_orders/"
    data=pd.read_csv('/users/hua/prime_orders.csv')
    #data['OrderId']=data['OrderId'].astype('int')
    data['delivery_month']=data['arrive_date'].apply(lambda x: str(x)[0:7].replace('-',''))
    months=list(pd.unique(data['delivery_month']))
    #data['ArrivedShanghaiDate']=pd.to_datetime(data['ArrivedShanghaiDate'])
    #pk=data.groupby('PackageNumber').agg({'OrderId':'nunique','VendorName':'nunique','ArrivedShanghaiDate':'max'})
    """pk.columns=['订单数','卖家数','最后订单入库日期']
    data=data.merge(pk,how='left',left_on='PackageNumber',right_index=True)
    data.columns=['OrderId', 'OrderNumber', 'PackageNumber', 'ProductName', 'ProductUrl',
       'VendorName', 'Qty', '付款时间', '采购时间',
       '入库时间', '发送目的国家时间', '到达目的国家时间',
       'ETA开始时间', 'ETA截止时间', 'WarehouseCode', 'Weight',
       'EstWeight', '商品来源网站域', 'ShipmentTypeName','体积重','预估体积重','chargeWeight', 'delivery_month', '封箱日期',
       '一级展示目录', '二级展示目录',
       '三级展示目录', '订单数', '卖家数','最后订单入库日期']"""
    for m in months:
        tp=data[(data['delivery_month']==m)]
        xname=filePath+m+'.xlsx'
        writer = pd.ExcelWriter(xname, engine='xlsxwriter',options={'strings_to_urls': False})
        print(xname)
        del tp['delivery_month']
        tp.to_excel(writer,index=False)
        writer.close()
    #checked.columns=['payment_number','last_total']
    #data=pd.read_excel("/users/hua/documents/jiehui2015.xlsx")
'''    data['delivery_month']=data['delivery_date'].apply(lambda x: x[0:7])
    catalog=list(pd.unique(data['catalog_code']))
    catalog.remove('AU')
    months=list(pd.unique(data['delivery_month']))
    for c in catalog:
        for m in months:
            tp=data[(data['catalog_code']==c)&(data['delivery_month']==m)]
            print(len(tp),math.ceil(len(tp)/15000))
            for i in range(math.ceil(len(tp)/15000)):
                xname='/users/hua/documents/temp/jiehui_cq_2015/'+c+'_'+m.replace("-","")+'_'+str(i)+'.xlsx'
                print(xname)
                out=tp.iloc[i*15000:(i+1)*15000-1]
                writer = pd.ExcelWriter(xname, engine='xlsxwriter',options={'strings_to_urls': False})
                out.to_excel(writer,index=False)
                writer.close()
                print(out.shape)
                
                
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

 