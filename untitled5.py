import pandas as pd
#me=pd.read_csv('/users/hua/jiehui201701-04.csv')
#yang=pd.read_excel('/users/hua/downloads/SG_2017-01_16.xls')
#jn=me.merge(yang,how='inner',left_on='payment_number',right_on='PaymentNumber')
prepay=pd.read_csv('/users/hua/prepay.csv')
p_sort= prepay.sort_values(by=['customerid','createdate'])[['customerid','createdate','total','prepay','notes']]
tp=prepay[prepay.customerid==1073377]
tp=tp.sort_values(by='createdate')