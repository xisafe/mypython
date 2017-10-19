import os
import time
import pandas as pd
from etlpy.cons import conn as cons
main_path='E:/sljr/project/5-开发文档/Script/hive/'
if not os.path.exists(main_path):
    main_path='/home/bigdata/'
#以下为最终结果对应的中文名字
now = time.time()
today=time.strftime('%Y-%m-%d',time.localtime(now))
yesterday=time.strftime('%Y-%m-%d',time.localtime(now - 24*60*60))
cols_cn_map={'db_name':'数据库', 
         'tb_name':'表名', 
         'create_time':'表创建时间', 
         'row_num':'表行数', 
         'total_size':'表空间大小',
         'comments':'表备注',
         'last_ddl_time':'表上次操作时间', 
         'part_name':'表最近分区',
         'sh_files':'表对应shell文件', 'sh_cmd':'表对应命令',
         'job_type':'作业类型', 'inc_flag':'增量标识', 
         'sql_file':'表对应sql文件', 
         'sql_tb_cn':'表对应sql中文名称', 
         'create_by':'表对应sql创建人',
         'cfg_denpend':'表对应ETL依赖配置', 
         'cfg_target_tb':'表ETL完成后生成结果文件', 
         'job_name':'在oozie中配置的作业',
         'job_last_time':'上次执行完成时间',
         'job_next_time':'下次执行时间',
         'job_used_times':'表对应etl上次用时秒',
         'frequency':'执行频率',
         'run_rank':'执行顺序',
         'rerun_sh':'重跑命令',
         'run_time':'执行时间安排'
            }
#插入mysql的字段顺序
insert_cols=['db_name', 'tb_name', 'create_time', 'row_num', 'total_size',
       'comments', 'last_ddl_time', 'part_name', 'sh_files', 'sh_cmd',
       'job_type', 'inc_flag', 'rerun_sh', 'sql_file', 'sql_tb_cn',
       'create_by', 'cfg_denpend', 'cfg_target_tb', 'job_name',
       'job_last_time', 'job_next_time', 'job_used_times', 'frequency',
       'run_time', 'run_rank', 'oper_date'] 
def readlogs(path):
    tables_list=pd.DataFrame(columns=['tables_name','start_time','end_time','diff_times','error_flag','log_file'])
    nums=1
    for i in os.walk(path):
        for files in i[2]:
            filepath=i[0]+'/'+files
            if(os.path.isfile(filepath)) and os.path.splitext(filepath)[1]=='.log':
                try:
                    table_name=files[0:-16]
                    #print(table_name)
                    if table_name.startswith('sqoop_'):
                        table_name=table_name[6:]
                    if table_name.startswith('job_'):
                        table_name=table_name[4:]    
                    if table_name.endswith('_daily'):
                        table_name=table_name[0:-6]
                    #table_name=table_name.replace('sqoop_','').replace('job_','').replace('_daily','')
                    if table_name.endswith('_inc'):
                        table_name=table_name[0:-4]
                    with open(filepath, 'r',encoding='utf-8') as f:  #打开文件
                        lines = f.readlines() #读取所有行
                        first_line = lines[0] #取第一行
                        error_flag=''
                        for line in lines:
                            if 'ERROR' in line or '[ERROR]' in line or 'FAILED:' in line:
                                error_flag='ERROR'               
                        start_time=time.strptime(first_line[1:20], "%Y-%m-%d %H:%M:%S")
                        last_line = lines[-1] #取最后一行
                        if('ERROR' in last_line or 'error' in last_line):
                            print('错误作业')
                            diff_times=-1
                            error_flag='ERROR'
                        else:
                            end_time=time.strptime(last_line[1:20], "%Y-%m-%d %H:%M:%S")
                            diff_times=time.mktime(end_time)-time.mktime(start_time)
                            
                        tables_list.loc[nums]=[table_name,first_line[1:20],time.strftime("%Y-%m-%d %H:%M:%S",end_time),diff_times,error_flag,filepath]
                    nums=nums+1
                except Exception as e:
                    #print('str(Exception):\t', str(Exception))
                    print (files,'\t error :\t\t',str(e))        
    return tables_list

def readConf(path):
    tables_list=pd.DataFrame(columns=['sql_file','cfg_denpend','cfg_target_tb'])
    except_list=['cdi_area_daily.properties','sqoop_handler.properties']
    nums=1
    for i in os.walk(path):
        #rs={}
        for files in i[2]:
            filepath=i[0]+'/'+files
            try:
                if(os.path.isfile(filepath)) and os.path.splitext(filepath)[1]=='.properties' and files not in except_list:
                    table_name=files.replace('.properties','')+'.sql'
                    #print('-'*90,files.replace('.properties',''))
                    keys=''
                    values=[]
                    with open(filepath, 'r') as f:  #打开文件
                        lines = f.readlines() #读取所有行
                        file_rs={}
                        for line in lines:
                            line=line.strip()
                            if len(line)>1 and not line.startswith('#'):
                                if '[' in line:
                                    if len(keys)>1:
                                        file_rs[keys]=values
                                    keys=line
                                    values=[]
                                else:
                                    if line not in['@DAILY']:
                                        values.append(line)
                    tables_list.loc[nums]=[table_name,file_rs['[dependence]'],file_rs['[results]'][0]]
                    #rs[table_name]= file_rs
                    nums=nums+1
            except Exception as e:
                    #print('str(Exception):\t', str(Exception))
                    print (files,'\t error :\t\t',str(e))     
    return tables_list
def readShell(path):
    tables_list=pd.DataFrame(columns=['sh_files','sh_cmd','job_type','inc_flag','rerun_sh'])
    except_list=['cdi_area_daily.sh','sqoop_test.sh']
    nums=1
    for i in os.walk(path):
        for files in i[2]:
            filepath=i[0]+'/'+files
            try:
                if(os.path.isfile(filepath)) and os.path.splitext(filepath)[1]=='.sh' and files not in except_list:
                    with open(filepath, 'r') as f:  #打开文件
                        lines = f.readlines() #读取所有行
                        for line in lines:
                            raw_line=line.strip()#.copy()
                            line=line.strip()
                            inc_flag=''
                            sql_files=''
                            dtype=''
                            if 'hive_sql_handler' in line:
                                dtype='hive'
                                sql_files=line[line.find('sql/')+4:]
                                tables_list.loc[nums]=[files,sql_files,dtype,inc_flag,raw_line]
                                nums=nums+1
                            if 'sqoop_handler' in line:
                                dtype='sqoop'
                                line=line[line.find('.ksh')+5:]
                                if line.endswith('inc'):
                                    inc_flag='增量'
                                sql_files=line.split(' ')[0].replace('_102','')+'_'+line.split(' ')[1]
                                tables_list.loc[nums]=[files,sql_files,dtype,inc_flag,raw_line]
                                nums=nums+1
            except Exception as e:
                    #print('str(Exception):\t', str(Exception))
                    print (files,'\t error :\t\t',str(e))     
    return tables_list
def readSql(path):
    tables_list=pd.DataFrame(columns=['sql_file','sql_tb_cn','create_by'])
    except_list=['cdi_area_daily.sql']
    nums=1
    for i in os.walk(path):
        for files in i[2]:
            filepath=i[0]+'/'+files
            try:
                #print(files)
                if files not in except_list and os.path.isfile(filepath) and files.endswith('.sql'):
                    with open(filepath, 'r',encoding='utf-8') as f:  #打开文件
                        lines = f.readlines() #读取所有行
                        target_tb_cn=''
                        create_by=''
                        for line in lines[0:20]:
                            line=line.strip()
                            if 'see:' in line:
                                target_tb_cn=line[line.find(':')+1:].strip()
                            #if 'name:' in line:
                            #    target_tb=line[line.find(':')+1:].strip()
                            if 'author:' in line:
                                create_by=line[line.find(':')+1:].strip()
                    tables_list.loc[nums]=[files,target_tb_cn,create_by]
                    nums=nums+1
            except Exception as e:
                    #print('str(Exception):\t', str(Exception))
                    print (files,'\t error :\t\t',str(e))     
    return tables_list
def getOozie():
    oozie=cons.meta('oozie')
    sql_txt="""
            SELECT
            	ifnull(w.app_name, c.app_name) job_name,
            	c.last_modified_time job_last_time,
            	c.next_matd_time job_next_time,
            	w.end_time - w.start_time job_used_times,c.frequency
            FROM
            	coord_jobs c
            LEFT JOIN coord_actions j ON c.id = j.job_id
            AND c.last_action_number = j.action_number
            LEFT JOIN wf_jobs w ON w.id = j.external_id
            WHERE
            	c.user_name = 'hue'
            AND c.`status` = 'RUNNING'
            """
    oz_df=pd.read_sql(sql_txt,oozie)
    return oz_df
def getHive():
    engine=cons.meta('hive')
    sql_txt="""
            SELECT 
                d.`NAME` db_name,
                TBL_NAME tb_name,
                FROM_UNIXTIME(CREATE_TIME) create_time,
                ifnull(p.row_num,0)+ifnull(pt.row_num,0) row_num,
                ifnull(p.total_size,0)+ifnull(pt.total_size,0) total_size,
                p.comments,
                case when pt.last_ddl_time>p.last_ddl_time then pt.last_ddl_time else p.last_ddl_time end last_ddl_time,
                -- case when pt.last_modified_time>p.last_modified_time then pt.last_modified_time else p.last_modified_time end last_modified_time,
                pt.part_name
             FROM tbls t
            INNER JOIN dbs d on t.DB_ID=d.DB_ID and d.`NAME` in('sdd','cdi','app')
            LEFT JOIN(select tbl_id,
                            max(if(PARAM_KEY='comment',PARAM_VALUE,null)) comments,
                            max(if(PARAM_KEY='numRows',PARAM_VALUE,'')) row_num,
                            max(if(PARAM_KEY='rawDataSize',PARAM_VALUE,'')) raw_data_size,
                            max(if(PARAM_KEY='totalSize',PARAM_VALUE,'')) total_size,
                            FROM_UNIXTIME(max(if(PARAM_KEY='transient_lastDdlTime',PARAM_VALUE,''))) last_ddl_time,
                            FROM_UNIXTIME(max(if(PARAM_KEY='last_modified_time',PARAM_VALUE,''))) last_modified_time,
                            max(if(PARAM_KEY='last_modified_by',PARAM_VALUE,null)) last_modified_by
                    from TABLE_PARAMS GROUP BY tbl_id) p on t.TBL_ID=p.tbl_id
            left JOIN(
                    SELECT 
                    p.TBL_ID,
                    sum(k.raw_data_size) raw_data_size,
                    sum(k.row_num) row_num,
                    sum(k.total_size) total_size,
                    max(p.PART_NAME) part_name,
                    max(k.last_ddl_time) last_ddl_time,
                    max(k.last_modified_time) last_modified_time
            from partitions p
            LEFT JOIN(
                    select PART_ID,
                    max(if(PARAM_KEY='numRows',PARAM_VALUE,'')) row_num,
                    max(if(PARAM_KEY='rawDataSize',PARAM_VALUE,'')) raw_data_size,
                    max(if(PARAM_KEY='totalSize',PARAM_VALUE,'')) total_size,
                    FROM_UNIXTIME(max(if(PARAM_KEY='transient_lastDdlTime',PARAM_VALUE,''))) last_ddl_time,
                    FROM_UNIXTIME(max(if(PARAM_KEY='last_modified_time',PARAM_VALUE,''))) last_modified_time
                     from partition_params GROUP BY PART_ID) k on p.PART_ID=k.PART_ID
            GROUP BY p.TBL_ID) pt on t.TBL_ID=pt.tbl_id
            """
    oz_df=pd.read_sql(sql_txt,engine)
    return oz_df
def cols_to_cn(cols):
    cols_cn=[]
    for c in cols:
        if c in cols_cn_map.keys():
            cols_cn.append(cols_cn_map[c])
        else:
            cols_cn.append(c)
    return cols_cn      
def get_crontab(st):
    if pd.notnull(st):
        tp=st.split(' ')
        times=tp[1].zfill(2)+':'+tp[0].zfill(2)
        if tp[3]=='*' and tp[4]=='*':
            return '每日 '+times
        elif tp[3]!='*':
            return '每月 '+times
        elif tp[4]!='*':
            return '每周 '+times
        else :
            return '每日 '+times
    else:
        return ''
    
if __name__ == '__main__':  

    cfg_rs=readConf(main_path+'/cfg') #读取配置文件
    sh_rs=readShell(main_path+'/bin') #读取执行文件shell
    sql_rs=readSql(main_path+'/sql') # 读取sql文件
    log_rs=readlogs(main_path+'/log/'+today) #读取日志文件
    sql_rs=sql_rs.merge(cfg_rs,how='left',on='sql_file') 
    rs=sh_rs.merge(sql_rs,how='outer',left_on='sh_cmd',right_on='sql_file')
    rs['cfg_target_tb']=rs['cfg_target_tb'].fillna(rs['sh_cmd'])
    oz_df=getOozie() # oozie元数据
    oz_df['run_time']=oz_df['frequency'].apply(get_crontab)
    oz_df['frequency']=oz_df['run_time'].apply(lambda x:x.split(' ')[0])
    oz_df['run_rank']=oz_df['run_time'].rank(method ='dense')
    sh_list=rs['sh_files'].drop_duplicates().dropna()
    sh_list_key=sh_list.apply(lambda x:x.replace('.sh','').replace('_inc',''))
    sh_oz_map=pd.DataFrame({'sh_files':sh_list,'sh_key':sh_list_key})
    sh_oz_map=sh_oz_map.merge(oz_df,how='left',left_on='sh_key',right_on='job_name')
    del sh_oz_map['job_last_time'],sh_oz_map['job_next_time'],sh_oz_map['job_used_times'],sh_oz_map['frequency'],sh_oz_map['run_time'],sh_oz_map['run_rank']
    for i in range(sh_oz_map.shape[0]):
         if pd.isnull(sh_oz_map.loc[i,'job_name']):
             keys=sh_oz_map.loc[i,'sh_key']
             #print(keys,sh_oz_map.loc[i,'job_name'])
             for j in range(oz_df.shape[0]):
                 if keys in oz_df.loc[j,'job_name']:
                     sh_oz_map.loc[i,'job_name']=oz_df.loc[j,'job_name']
                     
    sh_oz_map=sh_oz_map.merge(oz_df,how='left',on='job_name')    
    del sh_oz_map['sh_key'],i,j,keys,sh_list,sh_list_key #删除无效字段
    rs=rs.merge(sh_oz_map,how='left',on='sh_files')
    hive_df=getHive()
    sch_rs=rs[pd.notnull(rs['job_name'])] #在执行计划中的
    last_rs=hive_df.merge(sch_rs,how='left',left_on='tb_name',right_on='cfg_target_tb')
    last_rs['sh_files']=last_rs['sh_files'].fillna('无配置shell')
    last_rs['job_name']=last_rs['job_name'].fillna('暂无定时配置')
    last_rs['comments']=last_rs['comments'].fillna(last_rs['sql_tb_cn'])
    etl_data=cons.meta('etl_data')
    table_to_jobs=last_rs.copy()
    last_rs['oper_date']=today
    #result to mysql
    etl_data.execute("delete from etl_job_set where oper_date='{0}'".format(today))
    insert_rs=last_rs[insert_cols].copy()
    insert_rs=insert_rs.astype('str')
    insert_rs.to_sql(name='etl_job_set',con=etl_data,if_exists='append',index=False)
    etl_data.execute("delete from etl_log_sum where oper_date='{0}'".format(today))
    log_rs['oper_date']=today
    log_rs[['tables_name', 'start_time', 'end_time', 'diff_times', 'error_flag',
       'log_file', 'oper_date']].to_sql(name='etl_log_sum',con=etl_data,if_exists='append',index=False)
    table_to_jobs=table_to_jobs.sort_values(by=['run_rank','sh_files'])
    table_to_jobs.columns=cols_to_cn(table_to_jobs.columns)
    del table_to_jobs['上次执行完成时间'],table_to_jobs['表空间大小'],table_to_jobs['下次执行时间'],table_to_jobs['表对应命令'],table_to_jobs['表对应etl上次用时秒']
    del table_to_jobs['表行数'],table_to_jobs['表上次操作时间'],table_to_jobs['表最近分区'],table_to_jobs['表ETL完成后生成结果文件'],table_to_jobs['表创建时间'],table_to_jobs['表对应sql中文名称']
    excel_writer = pd.ExcelWriter(main_path+'/ETL表配置文档.xlsx', engine='xlsxwriter',options={'strings_to_urls': False})
    for i in list(table_to_jobs['数据库'].drop_duplicates()):
        table_to_jobs[table_to_jobs['数据库']==i].to_excel(excel_writer,index=False,sheet_name=i)
    excel_writer.close()
    #table_to_jobs.to_excel('e:/ETL表配置文档.xlsx',index=False,sheet_name='表')
     
    
             
                 
        
    
