# -*- coding: utf-8 -*-
from cons import conn as cons
import pandas as pd
import os
import time
from multiprocessing import Process,Pool
target_dir='e:/files' # 目标路径
now = time.time()
today=time.strftime('%Y-%m-%d',time.localtime(now))
yesterday=time.strftime('%Y-%m-%d',time.localtime(now - 24*60*60))
list_cons=cons.cons_map()
list_sql=pd.read_excel('e:\db_to_file.xlsx',sheetname='Sheet1')
list_sql['groups']=list_sql.index%4
def tb_to_csv(db='xfqz',tb='table_name',sql_txt='select 1',op_date=today):
    try:    
        if db not in list_cons.keys():
            raise Exception("数据库连接不存在：!", db)
        engine=list_cons[db]
        tb_data = pd.read_sql_query(sql_txt,con= engine)
        path=target_dir+'/'+op_date+'/'+db+'_'+tb+'.txt'
        if not os.path.isdir(target_dir+'/'+op_date):
            os.makedirs(target_dir+'/'+op_date)
        tb_data.to_csv(path_or_buf=path,sep='\001',index=False)
    except Exception as e:
        print('str(Exception):\t', str(Exception))
        print ('str(e):\t\t', str(e))        
def workers(pro_data):
    for i in  pro_data.itertuples():
        db=i[4]
        tb=i[2]
        sql_txt=i[3]
        print('',i[-1],'-',tb)
        tb_to_csv(db,tb,sql_txt,op_date='20170920')
if __name__ == '__main__':
    processes_num=4
    today_nuix = now - (now % 86400) + time.timezone
    start = time.time()
    workers(list_sql)
    end = time.time()
    print(end-start)
        #print ('e.message:\t', e.)
        #print ('traceback.format_exc():\n%s' % traceback.format_exc())