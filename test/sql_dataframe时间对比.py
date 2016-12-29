# encoding=utf-8
import pandas as pd
from myworks import cv_dw
import datetime
dates = pd.date_range('2015-06-01', periods=-10,freq="D")
mssql = cv_dw.MSSQL()
sql2 = """SELECT Appdate date1,ActualMerCreditDate date2,count(1) num FROM [dbo].[Base_CashadvInf]
where Status in('还款清算','关闭') and Appdate>'2015-08-01' GROUP by Appdate, ActualMerCreditDate ORDER BY Appdate """
df = pd.read_sql(sql2, mssql.conn)
# print df[u'资料审核完成'] # print df.columns # df.index pd.read_sql(sql2, mssql.conn, index_col="DDate")
begintime=datetime.datetime.now()
def getworkdays(date1,date2):
    mssql2 = cv_dw.MSSQL()
    sq="SELECT sum(iswork)-1 days FROM t_dim_day where DDate BETWEEN '%s' and '%s'" % (date1, date2)
    #print(sq)
    days=mssql2.ExecQuery(sq)
    return days[0][0]
# df['cumpro']=df.cumsum()['num']/5915 df['pro']=df['num']/5915
th=df.copy()
th['f'] = 0
for row in th.index:
      th.at[row,'f']=getworkdays(th.loc[row, 'date1'], th.loc[row, 'date2'])
print th
#rs=[]
#cur=mssql.ExecQuery(sql2)
#for row in cur:
    #  rs2 = list(row)
   #   #print type(row[0]),row[1]
  #    rs2.append(getworkdays(row[0], row[1]))
 #     rs.append(rs2)
#df2=pd.DataFrame(rs)
#print df2
endtime=datetime.datetime.now()
print("时间间隔:%s")%(str(endtime-begintime)+"\r\n")



