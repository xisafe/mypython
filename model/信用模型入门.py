import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
from sklearn.preprocessing import StandardScaler # for scaling the features
from sklearn.model_selection import train_test_split # for splitting the data into test and train data
#import tensorflow as tf
import matplotlib.pyplot as plt
from sklearn.utils import shuffle
from sklearn.metrics import confusion_matrix
import seaborn as sns
import matplotlib.gridspec as gridspec
from sklearn.manifold import TSNE
#read the csv file to a dataframe 
df=pd.read_csv(r"C:\Users\xzh\Desktop\creditcard.csv")
#Not considering Time as a feature now.'Class' is the target
v_features=[x for x in df.columns if x not in ('Time','Class')]
#standardising the features
#df[features]=StandardScaler().fit_transform(df[features])
#以下为欺诈时间序列的分布图
f, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(12,4))
bins = 50
ax1.hist(df.Time[df.Class == 1], bins = bins)
ax1.set_title('Fraud')
ax2.hist(df.Time[df.Class == 0], bins = bins)
ax2.set_title('Normal')
plt.xlabel('Time (in Seconds)')
plt.ylabel('Number of Transactions')
plt.show()
#查看每个应变量的分布情况
df.hist(figsize=(15,15),bins=50)
v_features=[x for x in df.columns if x not in ('Time','Class')]
plt.figure(figsize=(12,28*4))
gs = gridspec.GridSpec(28, 1)
for i, cn in enumerate(df[v_features]):
    ax = plt.subplot(gs[i])
    sns.distplot(df[cn][df.Class == 1], bins=50,color='red')
    sns.distplot(df[cn][df.Class == 0], bins=50,color='green')
    ax.set_xlabel('')
    ax.set_title('histogram of feature: ' + str(cn))
plt.show()
#矩阵散点图
small = df[v_features]
#pd.scatter_matrix(small,figsize=(18,12))
#相关性，相关系数
cov = np.corrcoef(small.T)
plt.figure(figsize=(12,7))
img = plt.matshow(cov,cmap=plt.cm.winter)
#相关性矩阵图
plt.colorbar(img, ticks=[-1,0,1])
plt.show()
Fraud = df[df.Class == 1]#分组
Normal = df[df.Class == 0]#分组
X_train = Fraud.sample(frac=0.7) #训练数据集
X_train = pd.concat([X_train, Normal.sample(frac = 0.7)], axis = 0)
X_test = df.loc[~df.index.isin(X_train.index)] #测试数据集
Y_train=X_train.Class
Y_test=X_test.Class
X_train=X_train.drop(['Class','Time'], axis =1)
X_test=X_test.drop(['Class','Time'], axis =1)
#逻辑回归
from sklearn import metrics
import scipy.optimize as op
from sklearn.linear_model import LogisticRegression
from sklearn.cross_validation import KFold, cross_val_score
from sklearn.metrics import precision_recall_curve,auc,roc_auc_score,roc_curve,recall_score,classification_report 
lrmodel = LogisticRegression(penalty='l2')
y_pred_undersample_score = lrmodel.fit(X_train, Y_train)
fpr, tpr, thresholds = roc_curve(Y_test.values.ravel(),y_pred_undersample_score)
roc_auc = auc(fpr,tpr)
# Plot ROC
plt.title('Receiver Operating Characteristic')
plt.plot(fpr, tpr, 'b',label='AUC = %0.2f'% roc_auc)
plt.legend(loc='lower right')
plt.plot([0,1],[0,1],'r--')
plt.xlim([-0.1,1.0])
plt.ylim([-0.1,1.01])
plt.ylabel('True Positive Rate')
plt.xlabel('False Positive Rate')
plt.show()
print('model')
print(lrmodel)
ypredlr = lrmodel.predict(X_test)
print('confusion matrix')
print(metrics.confusion_matrix(Y_test, ypredlr))
print('classification report')
print(metrics.classification_report(Y_test, ypredlr))
print('Accuracy : %f' % (metrics.accuracy_score(Y_test, ypredlr)))
print('Area under the curve : %f' % (metrics.roc_auc_score(Y_test, ypredlr)))
#随机森林模型
from sklearn.ensemble import RandomForestClassifier
rfmodel = RandomForestClassifier()
rfmodel.fit(X_train, Y_train)
print('model')
print(rfmodel)
ypredrf = rfmodel.predict(X_test)
print('confusion matrix')
print(metrics.confusion_matrix(Y_test, ypredrf))
print('classification report')
print(metrics.classification_report(Y_test, ypredrf))
print('Accuracy : %f' % (metrics.accuracy_score(Y_test, ypredrf)))
print('Area under the curve : %f' % (metrics.roc_auc_score(Y_test, ypredrf)))
#支持向量机分类
from sklearn.svm import SVC
svcmodel = SVC(kernel='sigmoid')   # 'linear' kernel is veeeeeery slow
svcmodel.fit(X_train, Y_train)
print('model')
print(svcmodel)
ypredsvc = svcmodel.predict(X_test)
print('confusion matrix')
print(metrics.confusion_matrix(Y_test, ypredsvc))
print('classification report')
print(metrics.classification_report(Y_test, ypredsvc))
print('Accuracy : %f' % (metrics.accuracy_score(Y_test, ypredsvc)))
print('Area under the curve : %f' % (metrics.roc_auc_score(Y_test, ypredsvc)))

from sklearn.linear_model import LogisticRegression
from sklearn.cross_validation import KFold, cross_val_score
from sklearn.metrics import confusion_matrix,precision_recall_curve,auc,roc_auc_score,roc_curve,recall_score,classification_report 
def printing_Kfold_scores(x_train_data,y_train_data):
    fold = KFold(len(y_train_data),5,shuffle=False) 

    # Different C parameters
    c_param_range = [0.01,0.1,1,10,100]

    results_table = pd.DataFrame(index = range(len(c_param_range),2), columns = ['C_parameter','Mean recall score'])
    results_table['C_parameter'] = c_param_range

    # the k-fold will give 2 lists: train_indices = indices[0], test_indices = indices[1]
    j = 0
    for c_param in c_param_range:
        print('-------------------------------------------')
        print('C parameter: ', c_param)
        print('-------------------------------------------')
        print('')

        recall_accs = []
        for iteration, indices in enumerate(fold,start=1):

            # Call the logistic regression model with a certain C parameter
            lr = LogisticRegression(C = c_param, penalty = 'l2')

            # Use the training data to fit the model. In this case, we use the portion of the fold to train the model
            # with indices[0]. We then predict on the portion assigned as the 'test cross validation' with indices[1]
            lr.fit(x_train_data.iloc[indices[0],:],y_train_data.iloc[indices[0],:].values.ravel())

            # Predict values using the test indices in the training data
            y_pred_undersample = lr.predict(x_train_data.iloc[indices[1],:].values)

            # Calculate the recall score and append it to a list for recall scores representing the current c_parameter
            recall_acc = recall_score(y_train_data.iloc[indices[1],:].values,y_pred_undersample)
            recall_accs.append(recall_acc)
            print('Iteration ', iteration,': recall score = ', recall_acc)

        # The mean value of those recall scores is the metric we want to save and get hold of.
        results_table.ix[j,'Mean recall score'] = np.mean(recall_accs)
        j += 1
        print('')
        print('Mean recall score ', np.mean(recall_accs))
        print('')

    best_c = results_table.loc[results_table['Mean recall score'].idxmax()]['C_parameter']
    
    # Finally, we can check which C parameter is the best amongst the chosen.
    print('*********************************************************************************')
    print('Best model to choose from cross validation is with C parameter = ', best_c)
    print('*********************************************************************************')
    
    return best_c
best_c = printing_Kfold_scores(X_train,Y_train)