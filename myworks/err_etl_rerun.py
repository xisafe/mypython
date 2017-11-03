import os
import time
import confs
import pandas as pd
import subprocess
from  cons import conn as conn
#以下为最终结果对应的中文名字
now = time.time()
today=time.strftime('%Y-%m-%d',time.localtime(now))
yesterday=time.strftime('%Y-%m-%d',time.localtime(now - 24*60*60))
def re_run():
    engine=conn.meta('etl_data')
    sql_txt="""
           select 
           distinct
                t.db_name ,
                t.tb_name ,
                t.comments ,
                t.part_name ,
                t.sh_files ,
                t.rerun_sh,
                t.frequency,
                t.run_time ,
                s.error_flag ,
                s.log_file ,
                s.start_time AS start_time 
                from etl_log_sum s left join etl_job_set t 
                      on s.tables_name = t.tb_name and t.oper_date = curdate()
                LEFT JOIN etl_err_rerun e on s.tables_name=e.tb_name and s.start_time=e.start_time
                where s.oper_date = curdate() and s.error_flag ='error' and (e.oper_date is null or e.re_run_flag='error')
                 and s.start_time<CONCAT(CURRENT_DATE(),' 09:00:00') and t.job_type='sqoop'
                  order by s.start_time
            """
    err_df=pd.read_sql(sql_txt,engine)
    #排序很重要不然作业容易错误
    insert_sql="""insert into etl_err_rerun(tb_name,start_time,re_run_flag,oper_date,re_run_sh) values('{0}','{1}','{2}','{3}','{4}')"""
    if err_df.shape[0]>0:
        for i in range(err_df.shape[0]):
            tb_name=err_df.loc[i,'tb_name']
            start_time=err_df.loc[i,'start_time']
            rerun_sh=err_df.loc[i,'rerun_sh']
            #log_file=err_df.loc[i,'log_file']
            #print(tb_name,start_time,rerun_sh)
            popen = subprocess.Popen(rerun_sh, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            popen.wait()
            if popen.poll()==0:
                re_run_flag='success'
                confs.send_mail('{0}错误作业自动重跑成功'.format(tb_name),tb_name+'错误已经解决')
            else:
                re_run_flag='error'
                #confs.send_mail('{0}错误作业自动重跑失败'.format(tb_name),tb_name,log_file)
            #re_run_log=popen.stdout.read()
            #re_run_log=popen.stderr.readlines()
            #for t in re_run_log:
              #  print(t)
            #print(re_run_log)
            engine.execute(insert_sql.format(tb_name,start_time,re_run_flag,today,rerun_sh))
        print('重跑完成')
        return 1
    else:
        print('没有错误作业')
        return 0
    #return err_df

if __name__ == '__main__':  
    rs=re_run()
   

    
    
      
    
             
                 
        
    
