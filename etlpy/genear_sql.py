# -*- coding: utf-8 -*-
from cons import  conn as cons
import pandas as pd
def con_str(a):
    point=','
    return point.join(a)
    
if __name__ == '__main__':
    try:
        engine=cons.xfqz()
        table_alias='t'
        sql1="""
  SELECT  TABLE_SCHEMA db,TABLE_NAME tb,COLUMN_NAME cols
 FROM information_schema.COLUMNS c where TABLE_SCHEMA 
in('java_xingfuqianzhuang','sljr_bank','sljr_borrow','sljr_jrxj','sljr_payment','sljr_slsw')
and (TABLE_NAME not LIKE '%%_bak%%' or TABLE_NAME not LIKE '%%bak_%%')
 """
        df = pd.read_sql_query(sql1,con= engine)
        df['cols']=df['cols'].apply(lambda x:table_alias+'.'+x)
        uni_db_tb=df[['db','tb']].drop_duplicates()
        #for i in  uni_db_tb.iterrows():
            #print(i[1][0])
        tp=[]
        for i in  uni_db_tb.itertuples():
            m=','.join(df[(df['db']==i[1])&(df['tb']==i[2])]['cols']).lower()
            tp.append('select '+ m+' from '+i[2]+' as '+ table_alias+' limit 10000')
            #print(m)
        uni_db_tb['sql_txt']=tp
        #ty=df.groupby(by=['db','tb'])['cols'].join()
        
        #df.to_csv(path_or_buf='e:\ste.csv',sep='\001')
    except Exception as e:
        print('str(Exception):\t', str(Exception))
        print ('str(e):\t\t', str(e))
        print ('repr(e):\t', repr(e))
        #print ('e.message:\t', e.)
        #print ('traceback.format_exc():\n%s' % traceback.format_exc())