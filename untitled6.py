import pandas as pd 
dt=pd.read_csv('/users/hua/prime_customer.csv')
catalog=list(pd.unique(dt['cust_ctry_cd']))
months=list(pd.unique(dt['dtype']))
for c in catalog:
    for m in months:
        tp=dt[(dt['cust_ctry_cd']==c)&(dt['dtype']==m)]
        xname='/users/hua/documents/temp/'+m+'_'+c+'.xlsx'
        print(xname)
        writer = pd.ExcelWriter(xname, engine='xlsxwriter',options={'strings_to_urls': False})
        tp.to_excel(writer,index=False)
        writer.close()
#dt.to_excel('/users/hua/prime_customer修改后.xlsx')