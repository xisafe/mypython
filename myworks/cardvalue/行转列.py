#encoding:utf-8
from pandas import Series,DataFrame
a=[['刘玄德','男','语文',98.],['刘玄德','男','体育',60.],['关云长','男','数学',60.],['张飞','女','语文',100.],['关云长','男','语文',100.]]
af=DataFrame(a,columns=['name','sex','course','score'])
af=af.sort(['name'])
print af
af.set_index(['name','sex','course'],inplace='TRUE')
print af
t1=af.unstack(level=2)
print t1
t2=t1.mean(axis=1,skipna=True)
t1['平均分']=t2
t1.fillna(0)
