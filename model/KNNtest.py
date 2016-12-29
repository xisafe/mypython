# encoding=utf-8
import numpy as np
import operator
import string as str
inX=[0,0]
k=5
def CreateDataSet():
    group = np.array([[1.0,1.1],[1,1],[0,0],[0,0.1]])
    lables = ['A', 'A', 'B', 'B']
    return group,lables
group,labs= CreateDataSet()
def classsfy0(inX,dataSet,lables,k):
    dataSetSize=dataSet.shape[0]
    diffMat=np.tile(inX,(dataSetSize,1))-dataSet
    sqdiffMat=diffMat**2
    distance=sqdiffMat.sum(axis=1)**5
    sortDistIndex=distance.argsort()
    classCount={} # dict类型 key-value
    for i in range(k):
        votelabels=lables[sortDistIndex[i]]
        classCount[votelabels]=classCount.get(votelabels,0)+1  # get没找到就是0默认值
    sortedClassCount=sorted(classCount.items())

mytest=[]
lables=[]
try:
    fobj = open('F:\\360yun\\python\\shuapiao12306\\Ch02\\datingTestSet.txt', 'r')
except IOError, e:
    print"*** file open error:", e
else:
    # display contents to the screen
    for eachLine in fobj:
        temp=eachLine.replace('\n','').split("\t")
        #t =list ([int(temp[0]),float(temp[1]),float(temp[2])])
        mytest.append(map(float,temp[1:3]))  # temp[0:3]+0.00
        lables.append(temp[3])
    fobj.close()
mymat=np.array(mytest)

dataSetSize=mymat.shape[0]
diffMat=np.tile(inX,(dataSetSize,1))-mymat
diffMatSq=diffMat**2
distince=diffMatSq.sum(axis=1)**0.5
#print distince#.argsort()
#print min(distince),
#print np.tile([0,0],(6,3))
print len(mymat[0])
for i in range(2):
    print i
#for tp in mymat:
    #print tp
print [tp[1] for tp in mymat]

