# encoding=utf-8
import pandas as pd
from myworks import cv_dw
dates = pd.date_range('2015-06-01', periods=200,freq="D")
mssql = cv_dw.MSSQL()
sql2 = """select t.DDate,p.[意向],p1.[新申请],p2.[评估额度完成],p3.[资料审核完成],p4.[签约前审核完成],p5.[批准],p6.[放款] from t_dim_day t
LEFT JOIN(
SELECT creationTime,count(1) 意向 FROM Dim_leads where creationTime>DATEADD(d, -150,GETDATE()) GROUP BY creationTime) p on t.DDate=p.creationTime
LEFT JOIN(
select Appdate,count(1) 新申请 from Base_CashadvInf where Appdate >DATEADD(d, -150,GETDATE()) GROUP BY Appdate) p1  on t.DDate=p1.Appdate
LEFT JOIN(
select AssessDate,count(1) 评估额度完成 from t_fact_Status_FirstCheck where AssessDate>DATEADD(d, -150,GETDATE()) GROUP BY AssessDate)p2  on t.DDate=p2.AssessDate
LEFT JOIN(
SELECT CheckedTime,count(1) 资料审核完成 FROM  t_fact_Status_FirstCheck  where CheckedTime>DATEADD(d, -150,GETDATE()) GROUP BY CheckedTime)p3  on t.DDate=p3.CheckedTime
LEFT JOIN(select OperDate,count(1) 签约前审核完成 from t_fact_Status_CheckSuccess where OperDate>DATEADD(d, -150,GETDATE()) GROUP BY OperDate) p4  on t.DDate=p4.OperDate
LEFT JOIN(SELECT CheckSuccessDate,count(1) 批准 from t_fact_Status_CheckSuccess where CheckSuccessDate >DATEADD(d, -150,GETDATE()) GROUP BY CheckSuccessDate) p5  on t.DDate=p5.CheckSuccessDate
LEFT JOIN(select convert(varchar,[还款清算],23) operdate,count(1) 放款 from t_fact_CvMainStatusTime where [还款清算] >DATEADD(d, -150,GETDATE()) GROUP BY convert(varchar,[还款清算],23))p6  on t.DDate=p6.OperDate
where t.DDate BETWEEN DATEADD(d, -150,GETDATE()) and GETDATE()  and p.[意向]>100
ORDER BY t.DDate"""
df = pd.read_sql(sql2, mssql.conn)   # print df[u'资料审核完成'] print df.columns df.index pd.read_sql(sql2, mssql.conn, index_col="DDate")
fk = df[u'放款']
# i = fk.index.copy()
def beflist(tp,i):
    tp=list(tp)
    tp2=tp[i:]
    j=0
    while j<i :
        tp2.append(0)
        j = j+1
    return pd.Series(tp2)

df['lag1']=beflist(df[u'放款'],1)
df['lag2']=beflist(df[u'放款'],2)
df['lag3']=beflist(df[u'放款'],3)
print df.corr()
#for d in i:
  #  print d


