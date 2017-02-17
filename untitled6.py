import pandas as pd
import numpy as np    
from sklearn import tree      
from sklearn.externals.six import StringIO  
import pydotplus  
#参数初始化
inputfile = 'files/modelData/data.xls'    #这里输入你个人的文件路径
data = pd.read_excel(inputfile, index_col = u'序号') #导入数据

#数据是类别标签，要将它转换为数据
#用1来表示“好”、“是”、“高”这三个属性，用-1来表示“坏”、“否”、“低”
data[data == u'好'] = 1
data[data == u'是'] = 1
data[data == u'高'] = 1
data[data != 1] = -1
x = data.iloc[:,:3].as_matrix().astype(int)
y = data.iloc[:,3].as_matrix().astype(int)

from sklearn.tree import DecisionTreeClassifier as DTC
dtc = DTC(criterion='entropy') #建立决策树模型，基于信息熵
dtc.fit(x, y) #训练模型

#导入相关函数，可视化决策树。
from sklearn.tree import export_graphviz
x = pd.DataFrame(x)
#with open("tree.dot", 'w') as f:
#  f = export_graphviz(dtc, feature_names = x.columns, out_file = f)
dot_data = StringIO()  
tree.export_graphviz(dtc,feature_names=x.columns,out_file=dot_data)
print(dot_data.getvalue())
graph = pydotplus.graph_from_dot_data(dot_data.getvalue())  
graph.write_pdf("tree.pdf")   
#'''''准确率与召回率'''  
#precision, recall, thresholds = precision_recall_curve(y_train, clf.predict(x_train))  
#answer = clf.predict_proba(x)[:,1]  
#print(classification_report(y, answer, target_names = ['thin', 'fat']))  