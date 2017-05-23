import pandas as pd
import numpy
data=pd.read_excel('/users/hua/downloads/内部转账.xlsx')
#inlist=pd.read_excel('/users/hua/downloads/内部转账.xlsx',sheetname='内转账号')
#inlist=list(inlist['account'])
#inner=data[data['ReceiptAccount'].apply(lambda x: x in inlist)]
#outter=data[data['ReceiptAccount'].apply(lambda x: x not in inlist)]
outter['AccountTurnover']=outter['AccountTurnover'].apply(str)
outter.to_excel('/users/hua/downloads/内部转账_排除.xlsx')
 