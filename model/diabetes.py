import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.feature_selection import VarianceThreshold
def dataDesc(data):   #数据的基础信息展示
    var_types= pd.DataFrame(data.dtypes,columns=['v_types'])
    var_types['flag']=var_types['v_types'].apply(lambda x:('float' in str(x)) or ('int' in str(x)) or ('ouble' in str(x)))  
    obj_features=data[var_types[var_types['flag']==False].index]
    num_desc=data.describe().T
    num_features=list(var_types[var_types['flag']==True].index)
    for num in num_features:
        st=data[num].value_counts()
        num_desc.at[num,'unique']=st.shape[0]
        if st.shape[0]>0:
            num_desc.at[num,'top']=st.index[0]
            num_desc.at[num,'freq']=st.values[0] 
    if obj_features.shape[1]>0 :              
        obj_desc=obj_features.describe().T
        rs=pd.concat([obj_desc,num_desc])
    else:
        rs=num_desc
    rs=rs[['min','25%','50%','75%','max','mean','std','count','unique','top','freq' ]]                      
    rs['missing']=1-rs['count']/data.shape[0]
    print("===================各个变量的基本情况==================")
    print(rs.round())
    #cov = np.corrcoef(data[num_features].T)
    cov=data[num_features].corr().round(2)
    print("===================各数量变量的相关系数矩阵==================")
    print(cov)
    plt.figure(figsize=(8,8))
    img = plt.matshow(cov,cmap=plt.cm.winter,fignum=0)# plt.cm.winter
    plt.title('corr of variable')
    plt.colorbar(img, ticks=[-1,0,1])
    plt.show()
    plt.close()
    print("===================变量分布统计图==================")
    data.hist(figsize=(13,4*(data.shape[1]//3+1)),bins=30) #各个变量分布
    plt.show()
    plt.close()
    if len(num_features)<11:
        print("===================各数值变量间的散点关系图==================")
        pd.scatter_matrix(data[num_features],figsize=(18,12))
        plt.show()
        plt.close()
    else:
        print("数值变量超过10个，不统计各个变量间的散点关系图")
    num_features.remove('Target')
    rows=len(num_features)//3+1
    plt.figure(figsize=(13,4*rows))
    i=1
    for v in num_features:
        plt.subplot(rows,3,i)
        plt.plot( data['Target'],data[v],'or')
        plt.title(v+"  vs Target scatter")
        i=i+1
    print("===================自变量与目标变量的散点图==================")
    plt.show()
    plt.close()
    return rs,cov
if __name__ == '__main__':
#    data=loadDataSet()
    data=pd.read_table('D:\model_data\diabetes.txt')
    data.rename(columns={'Y':'Target'}, inplace = True)#设立目标变量 因变量
     #desc=dataDesc(std_data) #数据初步探索
    y=np.array(data['Target'])
    x_df=data.drop(['Target'],axis=1)
    feature_names=list(x_df.columns)
    X=StandardScaler().fit_transform(X=x_df,y=y) # 标准化 也可以归一化，但是标准化多
    st=VarianceThreshold(threshold=3).fit_transform(X=data,y=y)
    #pca = PCA(n_components='mle')  
    #pca.fit(data[['AGE', 'SEX', 'BMI', 'BP', 'S1', 'S2', 'S3', 'S4', 'S5', 'S6']])
    #gui_data=(data - data.min()) / (data.max() - data.min()) #归一化
   
    #data.hist(figsize=(15,15),bins=30) #各个变量分布
    #pd.scatter_matrix(data,figsize=(18,12))