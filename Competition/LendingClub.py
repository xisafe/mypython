import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
sns.set(context='notebook',style="ticks",palette="GnBu_d",font_scale=1.5,font='ETBembo',
        rc={"figure.figsize": (13, 6)})

def dataDesc(data):   #数据的基础信息展示
    var_types= pd.DataFrame(data.dtypes,columns=['v_types'])
    var_types['flag']=var_types['v_types'].apply(lambda x:('float' in str(x)) or ('int' in str(x)) or ('ouble' in str(x)))  
    obj_features=data[var_types[var_types['flag']==False].index]
    num_desc=data.describe().T
    obj_desc=obj_features.describe().T
    rs=pd.concat([obj_desc,num_desc])
    rs=rs[['min','25%','50%','75%','max','mean','std','count','unique','top','freq' ]]                      
    rs['missing']=1-rs['count']/data.shape[0]
    #rs.loc[:,'missing']=1-rs.loc[:,'count']/data.shape[0]
    for num in var_types[var_types['flag']==True].index:
        st=data[num].value_counts()
        rs.at[num,'unique']=st.shape[0]
        if st.shape[0]>0:
            rs.at[num,'top']=st.index[0]
            rs.at[num,'freq']=st.values[0] 
    return rs     
if __name__=='__main__':
    if 'data' not in dir():
        data = pd.read_csv('D:\LoanStats3b.csv',skiprows=0,header=1)
    des=dataDesc(data)                      
    data=data.drop(labels=des[des['missing']>0.95].index,axis=1)
    data.loan_status.value_counts()
    Current=data[data.loan_status=='Current'][['issue_d','loan_status','term','loan_amnt','funded_amnt','total_rec_prncp','last_pymnt_amnt','installment','last_pymnt_d','next_pymnt_d']]
    FullyPaid=data[data.loan_status=='Fully Paid'][['issue_d','loan_status','term','loan_amnt','funded_amnt','total_rec_prncp','last_pymnt_amnt','installment','last_pymnt_d','next_pymnt_d']]
    ChargedOff=data[data.loan_status=='Charged Off'][['issue_d','loan_status','term','loan_amnt','funded_amnt','total_rec_prncp','last_pymnt_amnt','installment','last_pymnt_d','next_pymnt_d']]
    Late16_30days=data[data.loan_status=='Late (16-30 days)'][['issue_d','loan_status','term','loan_amnt','funded_amnt','delinq_amnt','total_rec_prncp','last_pymnt_amnt','installment','last_pymnt_d','next_pymnt_d']]
    Late31_120days=data[data.loan_status=='Late (31-120 days)'][['issue_d','loan_status','term','loan_amnt','funded_amnt','total_rec_prncp','last_pymnt_amnt','installment','last_pymnt_d','next_pymnt_d']]
    InGracePeriod=data[data.loan_status=='In Grace Period'][['issue_d','loan_status','term','loan_amnt','funded_amnt','total_rec_prncp','last_pymnt_amnt','installment','last_pymnt_d','next_pymnt_d']]
    Default=data[data.loan_status=='Default'][['issue_d','loan_status','term','loan_amnt','funded_amnt','total_rec_prncp','last_pymnt_d','last_pymnt_amnt','installment','next_pymnt_d']]
    #sns.boxplot(data,orient="h",color="c")
    #sns.despine(trim=True,offset=10) 'last_pymnt_amnt','installment',
