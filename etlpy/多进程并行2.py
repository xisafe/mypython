# -*- coding: utf-8 -*-
from cons import conn as cons
import pandas as pd
import os
import time
from multiprocessing import Process
target_dir='e:/files' # 目标路径
now = time.time()
today=time.strftime('%Y-%m-%d',time.localtime(now))
yesterday=time.strftime('%Y-%m-%d',time.localtime(now - 24*60*60))
list_cons=cons.cons_map()
list_sql=pd.read_excel('e:\db_to_file.xlsx',sheetname='Sheet1')
list_sql['groups']=list_sql.index%4
#if not os.path.isdir(target_dir+'/'+op_date):
#    os.makedirs(target_dir+'/'+op_date)
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
        print('进程：',i[-1],'-',tb)
        tb_to_csv(db,tb,sql_txt,op_date='20170922')
if __name__ == '__main__':
    today_nuix = now - (now % 86400) + time.timezone
    start = time.time()
    list_sql=pd.read_excel('e:\db_to_file.xlsx',sheetname='Sheet1')
    list_sql['groups']=list_sql.index%4
    try:
        p0 = Process(target=workers, args=(list_sql[list_sql['groups']==0], ))
        p1 = Process(target=workers, args=(list_sql[list_sql['groups']==1],)) 
        p2 = Process(target=workers, args=(list_sql[list_sql['groups']==2], )) 
        p3 = Process(target=workers, args=(list_sql[list_sql['groups']==3],)) 
        p0.start()
        p1.start()
        p2.start()
        p3.start()
        p0.join()
        p1.join()
        p2.join() 
        p3.join()    
    except Exception as e:
        print('str(Exception):\t', str(Exception))
        print ('str(e):\t\t', str(e))      
    print("main process runned all lines,用时：...")
    end = time.time()
    print(end-start)
    #用时395秒
        #print ('e.message:\t', e.)
        #print ('traceback.format_exc():\n%s' % traceback.format_exc())