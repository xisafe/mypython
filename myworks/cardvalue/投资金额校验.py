# -*- coding: utf-8 -*-
#import etlpy.cons as cons
from etlpy.cons import conn as cons
#from cons import conn as cons
import pandas as pd
import os
import time
#from multiprocessing import Process
target_dir='e:/files' # 目标路径
now = time.time()
today=time.strftime('%Y-%m-%d',time.localtime(now))
yesterday=time.strftime('%Y-%m-%d',time.localtime(now - 24*60*60))
list_cons=cons.cons_map()
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
        
if __name__ == '__main__':
    today_nuix = now - (now % 86400) + time.timezone
    start = time.time()
    #list_sql=pd.read_excel('e:\db_to_file.xlsx',sheetname='Sheet1')
    #list_sql['groups']=list_sql.index%4
    try:
        engine=list_cons['zfxt']
        sql_txt="""SELECT 
                        i.user_id,
                        tend_return_amount 回款总额,
                        tend_amount 投资总额,
                        cash_wait 在投本金,
                        profit_yes 已收收益,
                        i.user_source
                         FROM `user_account` u
                        INNER JOIN `user` i on u.account_id=i.account_id and i.user_source=1
                """
        if 'tb_data' not in dir():
            tb_data = pd.read_sql_query(sql_txt,con= engine)
            tb_data['user_id']=tb_data['user_id'].astype('int')
        sql_txt="""select 
                        user_id,
                        sum(receive_capital_wait) total_tended_capital,
                        sum(interest_receive_wait) total_profit,
                        sum(case when `status`=2 then receive_capital_wait else 0 end ) reback_captial,
                        sum(case when `status`=2 then interest_receive_wait else 0 end ) total_profit_rec
                        from 
                        java_xingfuqianzhuang.borrow_recover_plan 
                        group by user_id;
                """    
        if 'plan_data' not in dir():
            plan_data = pd.read_sql_query(sql_txt,con= engine)       
    except Exception as e:
        print('str(Exception):\t', str(Exception))
        print ('str(e):\t\t', str(e))      
    print("main process runned all lines,用时：...")
    end = time.time()
    print(end-start)
    datas=tb_data.merge(plan_data,how='left',left_on='user_id',right_on='user_id')
    datas=datas.fillna(0)
    diffs=datas[datas['投资总额']!=datas['total_tended_capital']]