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
    data=pd.read_csv('/users/hua/documents/mykettle/6临时取数/orders.csv')
    data['OrderId']=data['OrderId'].astype('int')
    fx=pd.read_csv('/users/hua/documents/mykettle/6临时取数/fengxiang.csv')
    fx.columns=['OrderId','封箱日期']
    tbc=pd.read_csv('/users/hua/documents/mykettle/6临时取数/ezbuy_order.csv')
    del tbc['shpmt_type_nm']
    tbc.columns=['OrderId', 'ez_dis_prod_catg_lvl_1_nm',
       'ez_dis_prod_catg_lvl_2_nm', 'ez_dis_prod_catg_lvl_3_nm']
    data['delivery_month']=data['VerifyProductPayDate'].apply(lambda x: str(x)[0:7].replace('/',''))
    data=data.merge(fx,how='left',left_on='OrderId',right_on='OrderId')
    data=data.merge(tbc,how='left',left_on='OrderId',right_on='OrderId')
    filePath = "/users/hua/documents/temp/ezbuy_orders"
    months=list(pd.unique(data['delivery_month']))
    data['ArrivedShanghaiDate']=pd.to_datetime(data['ArrivedShanghaiDate'])
    pk=data.groupby('PackageNumber').agg({'OrderId':'nunique','VendorName':'nunique','ArrivedShanghaiDate':'max'})
    pk.columns=['订单数','卖家数','最后订单入库日期']
    data=data.merge(pk,how='left',left_on='PackageNumber',right_index=True)
    data.columns=['OrderId', 'OrderNumber', 'PackageNumber', 'ProductName', 'ProductUrl',
       'VendorName', 'Qty', '付款时间', '采购时间',
       '入库时间', '发送目的国家时间', '到达目的国家时间',
       'ETA开始时间', 'ETA截止时间', 'WarehouseCode', 'Weight',
       'EstWeight', '商品来源网站域', 'ShipmentTypeName','体积重','预估体积重','chargeWeight', 'delivery_month', '封箱日期',
       '一级展示目录', '二级展示目录',
       '三级展示目录', '订单数', '卖家数','最后订单入库日期']
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
'''          
    #maindata=data.copy()
    #data3=data.join(checked,on='payment_number',how='inner',rsuffix='last-')
    #engine= create_engine('mssql+pymssql://publicezbuy:Yangqiang100%@192.168.199.106:1433/ezfinance',echo = True)
    #y=pd.read_excel("/users/hua/documents/temp/jiehui2015-checkout/外汇结汇包裹201504-1000新币包裹.xlsx")
    #checked=eachFile(filePath)
    #readFile(filePath)
    #writeFile(filePathI)publicezbuy
 