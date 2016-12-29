#encoding=utf-8
import pandas as pd

from test import MSSQL_class

#server ="192.168.0.98\SQLSERVERDB"
#user = "sa"
#password = "!@#qweasdZXC"
#conn = pymssql.connect(server, user, password, "cv_dw")
dates= pd.date_range('2015-06-01', periods=200,freq="D")
#dict={0:"v0",1:"v1",2:"v2",3:"v3",4:"v4",5:"v5",6:"v6",7:"v7",8:"v8",9:"v9",10:"v10"}
#cursor=conn.cursor()
mssql = MSSQL_class.MSSQL()
sql="""select t.DDate,p.[意向],p1.[新申请],p2.[评估额度完成],p3.[资料审核完成],p4.[签约前审核完成],p5.[批准],p6.[放款] from t_dim_day t
LEFT JOIN(
SELECT waitCheckTime,count(1) 意向 FROM t_fact_leadsPassRate where waitCheckTime>DATEADD(d, -150,GETDATE()) GROUP BY waitCheckTime) p on t.DDate=p.waitCheckTime
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
#cursor.execute(sql)
cursor=mssql.ExecQuery(sql)
rs=[]
for row in cursor:
        rs.append(row)
#row = cursor.fetchone()
#while row:
    #print("Name=%s" % (row[0]))
    #rs.append(row)
   # row = cursor.fetchone()
#conn.close()
df2 = pd.read_sql(sql, mssql.conn)
df=pd.DataFrame(rs)
print df
print "222222222dfdd%s"%__name__