import os
import time
import pandas as pd
from etlpy.cons import conn as cons
path='E:/files'

def readlogs(path):
    tables_list=pd.DataFrame(columns=['tables_name','start_time','end_time','diff_times'])
    nums=1
    for i in os.walk(path):
        for files in i[2]:
            filepath=i[0]+'/'+files
            if(os.path.isfile(filepath)) and os.path.splitext(filepath)[1]=='.log':
                table_name=files[6:-16]
                print(table_name)
                with open(filepath, 'r') as f:  #打开文件
                    lines = f.readlines() #读取所有行
                    first_line = lines[0] #取第一行
                    start_time=time.strptime(first_line[1:20], "%Y-%m-%d %H:%M:%S")
                    last_line = lines[-1] #取最后一行
                    if('ERROR' in last_line or 'error' in last_line):
                        print('错误作业')
                        diff_times=-1
                    else:
                        end_time=time.strptime(last_line[1:20], "%Y-%m-%d %H:%M:%S")
                        diff_times=time.mktime(end_time)-time.mktime(start_time)
                    tables_list.loc[nums]=[table_name,start_time,end_time,diff_times]
                nums=nums+1
    return tables_list

def readConf(path):
    tables_list=pd.DataFrame(columns=['sql_file','cfg_denpend','cfg_target_tb'])
    except_list=['cdi_area_daily.properties','sqoop_handler.properties']
    nums=1
    for i in os.walk(path):
        #rs={}
        for files in i[2]:
            filepath=i[0]+'/'+files
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
                tables_list.loc[nums]=[table_name,file_rs['[dependence]'],file_rs['[results]']]
                #rs[table_name]= file_rs
                nums=nums+1
    return tables_list
def readShell(path):
    tables_list=pd.DataFrame(columns=['sh_files','sh_cmd','job_type','inc_flag'])
    except_list=['cdi_area_daily.sh','sqoop_test.sh']
    nums=1
    for i in os.walk(path):
        for files in i[2]:
            filepath=i[0]+'/'+files
            if(os.path.isfile(filepath)) and os.path.splitext(filepath)[1]=='.sh' and files not in except_list:
                with open(filepath, 'r') as f:  #打开文件
                    lines = f.readlines() #读取所有行
                    for line in lines:
                        line=line.strip()
                        inc_flag=''
                        sql_files=''
                        dtype=''
                        if 'hive_sql_handler' in line:
                            dtype='hive'
                            sql_files=line[line.find('sql/')+4:]
                            tables_list.loc[nums]=[files,sql_files,dtype,inc_flag]
                            nums=nums+1
                        if 'sqoop_handler' in line:
                            dtype='sqoop'
                            line=line[line.find('.ksh')+5:]
                            if line.endswith('inc'):
                                inc_flag='增量'
                            sql_files=line.split(' ')[0]+'_'+line.split(' ')[1]
                            tables_list.loc[nums]=[files,sql_files,dtype,inc_flag]
                            nums=nums+1
    return tables_list
def readSql(path):
    tables_list=pd.DataFrame(columns=['sql_file','sql_tb_cn','create_by'])
    except_list=['cdi_area_daily.sql']
    nums=1
    for i in os.walk(path):
        for files in i[2]:
            filepath=i[0]+'/'+files
            #print(files)
            if files not in except_list and os.path.isfile(filepath):
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
    return tables_list
def getOozie():
    oozie=cons.meta('oozie')
    sql_txt="""
            SELECT
            	ifnull(w.app_name, c.app_name) job_name,
            	c.last_modified_time job_last_time,
            	c.next_matd_time job_next_time,
            	w.end_time - w.start_time job_used_times
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

if __name__ == '__main__':  

    cfg='E://sljr//project//5-开发文档//Script//hive//cfg'
    cfg_rs=readConf(cfg)
    sh_rs=readShell('E:\\sljr\\project\\5-开发文档\\Script\\hive\\bin')
    sql_rs=readSql('E:\\sljr\\project\\5-开发文档\\Script\\hive\\sql')
    sql_rs=sql_rs.merge(cfg_rs,how='left',on='sql_file')
    rs=sh_rs.merge(sql_rs,how='outer',left_on='sh_cmd',right_on='sql_file')
    oz_df=getOozie()
    sh_list=rs['sh_files'].drop_duplicates()
    sh_list_key=sh_list.apply(lambda x:x.replace('.sh','').replace('_inc',''))
    sh_oz_map=pd.DataFrame({'sh_files':sh_list,'sh_key':sh_list_key})
    sh_oz_map=sh_oz_map.merge(oz_df,how='left',left_on='sh_key',right_on='job_name')
    del sh_oz_map['job_last_time'],sh_oz_map['job_next_time'],sh_oz_map['job_used_times']
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
    
             
                 
        
    
