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
        print('进程：',i[-1],'-',tb)
        tb_to_csv(db,tb,sql_txt,op_date='20170923')
if __name__ == '__main__':
    processes_num=4
    today_nuix = now - (now % 86400) + time.timezone
    start = time.time()
    list_sql=pd.read_excel('e:\db_to_file.xlsx',sheetname='Sheet1')
    list_sql['groups']=list_sql.index%4
    pool = Pool(processes = processes_num)
    res_l=[]
    for i in range(processes_num):
        msg = "hello %d" %(i)
        res=pool.apply_async(workers, (list_sql[list_sql['groups']==i], ))   #维持执行的进程总数为processes，当一个进程执行完毕后会添加新的进程进去
        res_l.append(res)
    print("==============================>") #没有后面的join，或get，则程序整体结束，进程池中的任务还没来得及全部执行完也都跟着主进程一起结束了

    pool.close() #关闭进程池，防止进一步操作。如果所有操作持续挂起，它们将在工作进程终止前完成
    pool.join()   #调用join之前，先调用close函数，否则会出错。执行完close后不会有新的进程加入到pool,join函数等待所有子进程结束

    print(res_l) #看到的是<multiprocessing.pool.ApplyResult object at 0x10357c4e0>对象组成的列表,而非最终的结果,但这一步是在join后执行的,证明结果已经计算完毕,剩下的事情就是调用每个对象下的get方法去获取结果
    for i in res_l:
        print(i.get()) #使用get来获取apply_aync的结果,如果是apply,则没有get方法,因为apply是同步执行,立刻获取结果,也根本无需get
    #try:
      
    #except Exception as e:
    #    print('str(Exception):\t', str(Exception))
    #    print ('str(e):\t\t', str(e))      
    
    end = time.time()
    print(end-start)
        #print ('e.message:\t', e.)
        #print ('traceback.format_exc():\n%s' % traceback.format_exc())