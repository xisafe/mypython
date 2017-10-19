# -*- coding: utf-8 -*-
import cons
import pandas as pd
import os
import time
def cons_map():
    cons_map=dict()
    for vars in dir(cons):
        if len(vars)>=4:
             try:
                 cons_map[vars]=eval('cons.{0}()'.format(vars))
             except Exception as e:
                 print('不是链接：',vars)
    return cons_map        
list_cons=cons_map()
def tb_to_csv(db='xfqz',tb='table_name',sql_txt='select 1',target_dir='e:/files',op_date='20170901'):
    if db not in list_cons.keys():
        raise Exception("数据库连接不存在：!", db)
    engine=list_cons[db]
    tb_data = pd.read_sql_query(sql_txt,con= engine)
    path=target_dir+'/'+op_date+'/'+db+'_'+tb+'.txt'
    if not os.path.isdir(target_dir+'/'+op_date):
        os.makedirs(target_dir+'/'+op_date)
    tb_data.to_csv(path_or_buf=path,sep='\001',index=False)
    
if __name__ == '__main__':
    today=time.strftime("%Y-%m-%d")
    start = time.time()
    list_sql=pd.read_excel('e:\db_to_file.xlsx',sheetname='Sheet1')
    #tb_to_csv(db='xfqz',tb='table_name',sql_txt='select 1',target_dir='e:/files',op_date='20170901')
    for i in  list_sql.itertuples():
        db=i[4]
        tb=i[2]
        sql_txt=i[3]  
        try:
            tb_to_csv(db,tb,sql_txt,op_date='20170914')
            #print(tb)
        except Exception as e:
            print('str(Exception):\t', str(Exception))
            print ('str(e):\t\t', str(e))
            print ('repr(e):\t', repr(e))
    end = time.time()
    print(end-start)
        #print ('e.message:\t', e.)
        #print ('traceback.format_exc():\n%s' % traceback.format_exc())