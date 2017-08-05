import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import pandas as pd
def dataDesc(data,isclass_ratio=0.05):   #数据的基础信息展示
    var_types= pd.DataFrame(data.dtypes,columns=['v_types'])
    var_types['flag']=var_types['v_types'].apply(lambda x:('float' in str(x)) or ('int' in str(x)) or ('ouble' in str(x)))  
    obj_features=data[var_types[var_types['flag']==False].index]
    num_desc=data.describe().T
    if obj_features.shape[1]>0:
        obj_desc=obj_features.describe().T
        rs=pd.concat([obj_desc,num_desc])
        rs=rs[['min','25%','50%','75%','max','mean','std','count','unique','Mode','Mode_freq' ]]
    rs=num_desc.copy()
    rs=rs[['min','25%','50%','75%','max','mean','std','count' ]]                      
    rs['miss_ratio']=1-rs['count']/data.shape[0]
    #rs.loc[:,'missing']=1-rs.loc[:,'count']/data.shape[0]
    for num in var_types[var_types['flag']==True].index:
        st=data[num].value_counts()
        rs.at[num,'unique']=st.shape[0]
        if st.shape[0]>0:
            rs.at[num,'Mode']=st.index[0]
            rs.at[num,'Mode_freq']=st.values[0]
    rs['unique_ratio']=rs['unique']/data.shape[0]
    rs['class_type']='连续'
    rs.loc[rs['unique_ratio']<isclass_ratio,'class_type']='分类'
    del rs['unique_ratio']
    return rs 
if __name__=='__main__':
    mydata=pd.read_csv('/users/hua/downloads/cs-training.csv')
    del mydata['ids']
    #mydata.columns=['y','x1','x2','x3','x4','x5','x6','x7','x8','x9','x10']
    desc=dataDesc(mydata)
    