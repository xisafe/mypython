import pandas as pd
from sklearn.cluster import KMeans
from sklearn.externals import joblib
import matplotlib.pyplot as plt
import numpy as np
from sklearn.preprocessing import StandardScaler
#from  matplotlib.colors import cnames
import random #import seaborn as sns
#colors=[u'blue',u'yellow',u'red',u'green',u'cyan',u'magenta',u'black']#colors.extend(filter(lambda x :len(x)<7 and x not in colors,cnames.keys())) # 设置不同颜色集合
def RandomSampling(dataMat,number): #无放回随机抽样
  try:
     slice = random.sample(dataMat, number)
     return slice
  except:
     print('sample larger than population')

def RepetitionRandomSampling(dataMat,number):	# 有放回随机抽样
  sample=[]
  for i in range(number):
     sample.append(dataMat[random.randint(0,len(dataMat)-1)])
  return sample
def SystematicSampling(dataMat,number):	#系统抽样无放回
     length=len(dataMat)
     k=length/number
     sample=[]
     i=0
     if k>0 :
         while len(sample)!=number:
             sample.append(dataMat[0+i*k])
             i+=1
         return sample
     else :
         return RandomSampling(dataMat,number)

def ReliefF(X,y,N=20,K=6,M=40):   #X,y(必须是分类或者类别变量)，N #执行次数 K=6 #最近的k个样本 M=40  抽样次数
    rows,cols=X.shape
    rows=rows*1.0  # mylabel=y #类别变量，因变量y
    plt.figure()
    Amax_min_diff=X.max(axis=0)-X.min(axis=0)
    dislabels=np.unique(y)  #list(pd.unique(mylabel)) # np.unique(mylabel)
    classSet={}
    labNumDict=pd.DataFrame(y,columns=['labels']).groupby(by='labels')['labels'].agg({u"nums":np.size}).to_dict()['nums']
    WA=[]
    for i in dislabels:
        classSet[i]=np.array(pd.unique(X[np.where(y==i)[0],:]).tolist())
    for lp in range(N):
        w=np.zeros(shape=(cols)) #[0]*(cols-1)
        for l in range(M):
            Rset=RandomSampling(X,1)[0]  #随机抽样
            Rtype=Rset[cols-1]              #抽出的样本类别
            Rdata=np.array(Rset[0:cols-1].tolist())   #样本数据
            for i in dislabels:
                Dsub=classSet[i]
                TopK=pd.DataFrame(((Dsub-Rdata)**2).sum(axis=1),columns=['sumSquare']).sort_values(by='sumSquare').head(K).index.tolist()
                if i==Rtype:
                    w=w-np.array((np.abs((Dsub[TopK]-Rdata))/Amax_min_diff).tolist()).sum(axis=0)
                else :
                    pc=(labNumDict[i]/rows)/(1-(labNumDict[Rtype]/rows))
                    w=w+pc*np.array((np.abs((Dsub[TopK]-Rdata))/Amax_min_diff).tolist()).sum(axis=0)
        w=list(w/(K*M))
        plt.plot(w)
        WA.append(w)
    WA=np.array(WA)
    pd.DataFrame(WA).to_csv('c:/temp.csv')
    rs = pd.DataFrame(WA.mean(axis=0), columns=['weight']).sort_values(by='weight',ascending=False)
    rs=rs[rs['weight']>-1]
    print(rs)
    plt.show()
    ranklist=list(rs.index)
    return ranklist #结果按权值从小到大排序
if __name__ == '__main__':
    data=pd.read_csv('D:\model_data\creditcard.csv')
    data.rename(columns={'Class':'Target'}, inplace = True)#设立目标变量 因变量
     #desc=dataDesc(std_data) #数据初步探索
    y=np.array(data['Target'])
    x_df=data.drop(['Target'],axis=1)
    feature_names=list(x_df.columns)
    X=StandardScaler().fit_transform(X=x_df,y=y)
#mydata=pd.read_csv('d:/breast-cancer-wisconsin.data',header=None,sep=',',na_values='?')
#mydata.columns=[u'id',u'块厚度',u'细胞大小均匀性',u'细胞形态均匀性',u'粘附力',u'细胞尺寸',u'裸核',u'Bland',u'正常核仁',u'核分裂',u'分类']
#mydata[6]=mydata[6].astype(float).fillna(mydata[6].mean())# 只有在column名字为纯数值的情况下可用
#mydata[10]= mydata[10].apply(lambda x:x==2 and 'x1' or 'x2')
#mymat=np.array(mydata.iloc[:,1:11])  #自变量数组
#rows,cols=mymat.shape
#RankList=ReliefF(mymat) ##结果按权值从小到大排序
#labNumDict=pd.DataFrame((mymat[:,cols-1]),columns=['labels']).groupby(by='labels')['labels'].agg({u"nums":np.size}).to_dict()['nums']




