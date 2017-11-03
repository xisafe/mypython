import pandas as pd
def file1():
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
def csv_to_excel(path,sep=','):
    dt=pd.read_csv(path,sep)
    outpath=path.replace('.csv','.xlsx')
    writer = pd.ExcelWriter(outpath, engine='xlsxwriter',options={'strings_to_urls': False})
    dt.to_excel(writer,index=False)
    writer.close()            
#dt.to_excel('/users/hua/prime_customer修改后.xlsx')
if __name__ == '__main__':
    filePath = "/users/hua/documents/temp/jiehui2015-checkout"
    dt=pd.read_csv('/users/hua/customer0719.csv',sep='\t')
    #xname='/users/hua/Marketing-friendsdeal_segment_list.xlsx'
    #writer = pd.ExcelWriter(xname, engine='xlsxwriter',options={'strings_to_urls': False})
    #dt.to_excel(writer,index=False)
    csv_to_excel('/users/hua/customer071917.csv')