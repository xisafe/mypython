import os
import time
import pandas as pd
from myworks.cons import conn as cons
#main_path='E:/sljr/project/5-开发文档/Script/hive/'
main_path='/home/bigdata/'
#以下为最终结果对应的中文名字
now = time.time()
today=time.strftime('%Y-%m-%d',time.localtime(now))
yesterday=time.strftime('%Y-%m-%d',time.localtime(now - 24*60*60))

def re_run():
    engine=cons.meta('hive')
    sql_txt="""
           SELECT * FROM etl_data.etl错误任务视图
           where 错误标示='error' and start_time<CONCAT(CURRENT_DATE(),' 08:00:00');
            """
    err_df=pd.read_sql(sql_txt,engine)
    if err_df.shape[0]>0:
        pass
    else:
        print('没有错误作业')
    return err_df

if __name__ == '__main__':  
    r=re_run()

    
    
      
    
             
                 
        
    
