import numpy as np
import matplotlib.pyplot as plt
from matplotlib import gridspec
from matplotlib.ticker import FuncFormatter
import pandas as pd
import matplotlib
def to_percent(y, position,flag=0):
    s = str(100 * y)
    if matplotlib.rcParams['text.usetex'] is True:
        return s + r'$\%$'
    else:
        return s + '%' 
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
def miss_data_process(mydata):
    desc=dataDesc(mydata)
    missing_var=desc[desc['miss_ratio']>0]
    for index,class_type in  missing_var.class_type.iteritems():
        if class_type=='连续':
            mydata.loc[pd.isnull(mydata[index]),index]=mydata[index].mean()
        else :
            mydata.loc[pd.isnull(mydata[index]),index]=mydata[index].mode()[0]
    return mydata
def plotDist(X,title='',bins_k=40):
    X=np.array(X)
    print(' '*15,'变量：',title,'分布情况如下图：')
    formatter = FuncFormatter(to_percent)
    plt.figure(figsize=(10, 6)) 
    plt.title(title)
    gs = gridspec.GridSpec(2, 1, height_ratios=[1,10]) 
    ax0 = plt.subplot(gs[0])
    ax0.boxplot(X,vert=False)
    plt.grid()
    ax1 = plt.subplot(gs[1])
    ax1.set_ylabel('hist distribution')
    ax1.hist(X, bins=bins_k, normed=False,color='g')
    plt.grid()
    ax2 = ax1.twinx()  # this is the important function
    ax2.set_ylabel(' cumulative distribution')
    plt.gca().yaxis.set_major_formatter(formatter)
    acc=ax2.hist(X, bins=bins_k, normed=True,color='red',cumulative=True,histtype='step')
    plt.annotate('Mean : '+format(X.mean(),'.2f')+'', xy=(-90, 1), xytext=(-90, 1))
    plt.annotate(' Std :' + format(X.std(),'.2f')+'', xy=(-90, 1), xytext=(-90, 0.95))
    plt.annotate(' Var :' + format(X.var(),'.2f')+'', xy=(-90, 1), xytext=(-90, 0.90))
    plt.tight_layout()
    plt.show()
    plt.close()
    return acc
def show_var_dist_plot(mydata,bins_k=20):
    for var in mydata.columns:
        tp=mydata[mydata[var]<mydata[var].mean()+10*mydata[var].std()][var]
        plotDist(tp,title=var,bins_k=bins_k)
def showCluster(dataSet, k, centroids, clusterAssment):  
    numSamples, dim = dataSet.shape  
    if dim != 2:  
        print("Sorry! I can not draw because the dimension of your data is not 2!")
        return 1  
  
    mark = ['or', 'ob', 'og', 'ok', '^r', '+r', 'sr', 'dr', '<r', 'pr']  
    if k > len(mark):  
        print("你的聚类多余10组，系统无法展示")  
        return 1  
  
    # draw all samples  
    for i in range(numSamples):  
        markIndex = int(clusterAssment[i])  
        plt.plot(dataSet[i, 0], dataSet[i, 1], mark[markIndex])  
  
    mark = ['Dr', 'Db', 'Dg', 'Dk', '^b', '+b', 'sb', 'db', '<b', 'pb']  
    # draw the centroids  
    for i in range(k):  
        plt.plot(centroids[i, 0], centroids[i, 1], mark[i], markersize = 12)  
    plt.show()
      
if __name__=='__main__':
    mydata=pd.read_csv('/users/hua/downloads/cs-training.csv') #,na_values='NULL'
    del mydata['ids']
    features=mydata.columns
    #mydata.columns=['y','x1','x2','x3','x4','x5','x6','x7','x8','x9','x10'] MonthlyIncome
    desc=dataDesc(mydata)
    mydata=miss_data_process(mydata)
    #m=pd.Dataframe(pd.cut(mydata.age,bins=30))
    from sklearn.cluster import KMeans
    tp=pd.DataFrame(mydata['age']).copy()
    tp['zeros']=0
    X=np.array(tp)
    kmeans = KMeans(n_clusters=9, random_state=0).fit(X)
    #showCluster(X,k=9,centroids=kmeans.cluster_centers_,clusterAssment=np.array(kmeans.labels_))  
    #plotDist(mydata['NumberOfDependents'],title='NumberOfDependents',bins_k=20)