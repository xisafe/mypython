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
    filePath = "/users/hua/documents/temp/jiehui2015-checkout"
    #checked.columns=['payment_number','last_total']
    data=pd.read_csv("/users/hua/jiehui201703-06.csv")
    data['delivery_month']=data['delivery_date'].apply(lambda x: x[0:7])
    catalog=list(pd.unique(data['catalog_code']))
    data['PackageStatusName']='已完成'
    data['Recepient']='service@65daigou.com'
    del data['delivery_date']#,data['etl_date']
    #catalog.remove('AU')
    months=list(pd.unique(data['delivery_month']))
    for c in catalog:
        for m in months:
            tp=data[(data['catalog_code']==c)&(data['delivery_month']==m)]
            #del tp['etl_date']
            print(len(tp),math.ceil(len(tp)/15000))
            for i in range(math.ceil(len(tp)/15000)):
                xname='/users/hua/documents/temp/jiehui201704_06/'+c+'_'+m.replace("-","")+'_'+str(i)+'.xlsx'
                print(xname)
                out=tp.iloc[i*15000:(i+1)*15000-1]
                writer = pd.ExcelWriter(xname, engine='xlsxwriter',options={'strings_to_urls': False})
                out.to_excel(writer,index=False)
                writer.close()
                print(out.shape)
            
"""
select 
d.payment_bill_id,
d.payment_number,
d.pay_date,
p.product_name,
d.bill_total,
d.product_total,
d.nick_name,
d.catalog_code,
'已完成' as PackageStatusName,
d.catalog_code as PriceSymbol,
d.sender,
'service@65daigou.com' as  Recepient,
p.ship_address,
p.ship_to_phone,
p.product_url,
d.warehouse_code,d.delivery_date,p.purchase_type
 FROM dw.ic_finance_exchange_detail d
inner join dw.ic_package_groupby_payment p
on d.payment_bill_id=p.payment_bill_id
where d.delivery_date>='2017-03-01' and d.delivery_date<='2017-07-01'

select 
d.payment_number,
d.pay_date,
p.product_name,
d.bill_total,
d.product_total,
d.nick_name,
d.catalog_code,
d.catalog_code as PriceSymbol,
d.sender,
p.ship_address,
p.ship_to_phone,
p.product_url,
d.warehouse_code,d.delivery_date,p.purchase_type
 FROM dw.ic_finance_exchange_detail d
inner join dw.ic_package_groupby_payment p
on d.payment_bill_id=p.payment_bill_id
where d.delivery_date>='2017-03-01' and d.delivery_date<='2017-07-01'
"""
 