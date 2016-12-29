# encoding=utf-8
import pandas as pd
from myworks import cv_dw
import datetime
mssql = cv_dw.MSSQL()
sql2 = """SELECT Appdate date1,ActualMerCreditDate date2,count(1) num FROM [dbo].[Base_CashadvInf]
where Status in('还款清算','关闭') and Appdate>'2015-08-01' GROUP by Appdate, ActualMerCreditDate ORDER BY Appdate """
#df = pd.read_sql(sql2, mssql.conn)
# print df[u'资料审核完成'] # print df.columns # df.index pd.read_sql(sql2, mssql.conn, index_col="DDate")
#th = df.copy()
#th['days'] = 0
#for row in th.index:
      #th.at[row, 'days'] = mssql.getworkdays(th.loc[row, 'date1'], th.loc[row, 'date2'])
#th.to_csv('e:\\360yun\\app_to_loan.csv')
d1=pd.read_csv('e:\\360yun\\app_to_loan.csv', index_col=0, header=0)
rs1=d1.groupby(['days']).sum()
rs1['cum']=rs1.cumsum()
total=d1['num'].sum()
rs1['cumprob']=rs1.cumsum()['num']/total
rs1['prob']=rs1['num']/total
#rs1['cump']=rs1['days'].cumsum()
final_rs=rs1[1:16]
sql3="""SELECT top 15 Appdate,count(1) count_num from Base_CashadvInf b INNER JOIN t_dim_day d on b.Appdate=d.DDate and d.iswork=1
 where Appdate<'2016-01-08' and Appdate>DATEADD(day, -40, '2016-01-08') GROUP BY Appdate ORDER BY Appdate DESC"""
pdata=pd.read_sql(sql3, mssql.conn)
pdata['rkrow']=range(1,16,1)
pdata.index=range(1,16,1)
predict=pd.merge(pdata, final_rs ,how='left', left_index=True , right_index=True,)

predict['loanout']=predict['prob']*predict['count_num']
predict.to_csv('e:\\fk.csv')
print  predict['loanout'].sum()
