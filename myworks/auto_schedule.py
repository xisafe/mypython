import os
import time
import pandas as pd
from  cons import conn as conn
import confs
import pymysql
from ssh import SSH_cmd as ssh_cmd
from ssh import SSH as ssh_con
main_path=confs.main_path
specia_list=['dim_cust_map'] #特殊依赖表

class auto_schedule(object):
    def __init__(self,group_num=10,frency='d',tar_ssh='ssh_uat'):
         self.group_num=group_num
         if frency in ['d','w','m']:#d 表示天 w 表示zhou m表示月   
             self.frency=frency
         else:
             print('frency 参数只能是 d(天),w(周),m(月) ')
             raise Exception("frency 参数只能是 d(天),w(周),m(月) ")                    
         self.group_name=frency+'_run_group'
         sshcon=ssh_con()
         self.ssh=ssh_cmd(sshcon.ssh_uat)
         if tar_ssh=='ssh_sc':
             self.ssh=ssh_cmd(sshcon.ssh_sc)
         
    def get_job_group(self): #获取现有的分组情况
        engine=conn.meta('etl_data')
        sql_txt="""
                    SELECT s.tb_name,s.sql_file,s.group_id,s.freq_type,
                    case when s.depend is null then e.cfg_denpend
                     when s.depend<>e.cfg_denpend and e.cfg_denpend<>'nan' then e.cfg_denpend
                    else s.depend end depend
                    FROM  job_group_set s 
                    LEFT JOIN etl_job_set e on s.tb_name=e.tb_name 
                    and e.oper_date=CURRENT_DATE() and e.job_type='hive'
                     where del_flag=0 and freq_type='{0}'
                  """
        job_group=pd.read_sql(sql_txt.format(self.frency),engine,index_col='tb_name')
        sql_txt="""
                    SELECT  group_id,max(rank_id) max_rank_id
                    FROM  job_group_set where  freq_type='{0}' group by group_id order by group_id
                  """ #,index_col='group_id'
        group_max_rank_id=pd.read_sql(sql_txt.format(self.frency),engine)
        return job_group.to_dict(),group_max_rank_id
    def get_group_usedtime(self): #获取作业用时情况，分组用时情况
        engine=conn.sljr_pg()
        sql_txt="""
                   select 
                        case when batch_type='hive' then batch_name||'.sql' else batch_name end batch_name,
                        avg(EXTRACT(EPOCH FROM (enddate-begindate))) used_time
                         from 
                        dc_stging.sljr_hive_batch_log 
                        where create_time>CURRENT_TIMESTAMP - interval '10 day' 
                        and error='success'  
                        GROUP BY case when batch_type='hive' then batch_name||'.sql' else batch_name end
                  """
        job_time=pd.read_sql(sql_txt,engine,index_col='batch_name')
        engine=conn.meta('etl_data')
        sql_txt="""
                    SELECT tb_name,sql_file,group_id,freq_type,depend
                    FROM  job_group_set where del_flag=0 and freq_type='{0}'
                  """
        job_group=pd.read_sql(sql_txt.format(self.frency),engine,index_col='tb_name')
        if job_group.shape[0]>0:
            job_group=job_group.merge(job_time,how='left',left_on='sql_file',right_index=True)
            job_group=job_group.fillna(job_group['used_time'].mean())
            job_group=job_group.groupby('group_id')['used_time'].sum()
            return job_group.to_dict(),job_time.to_dict()['used_time']
        else:
            group_used_time={}
            for i in range(self.group_num):
                group_used_time[i+1]=0
            return group_used_time,job_time.to_dict()['used_time'] 
    def group_sh(self):
        group_sh={}
        group_sql={}
        for i in range(self.group_num):
            group_file=self.group_name+str(i+1).zfill(2)+'.sh'
            filepath=confs.main_path_bin+group_file
            open(filepath, "wb").write(open(confs.main_path+'bin/template.sh', "rb").read())
            group_sh[i+1]=group_file
            group_sql[i+1]=[]
        return group_sh, group_sql
    
    def dict_sort_by_value(self,d,desc=False): # 数据字典key值按value排序
        items=d.items() 
        backitems=[[v[1],v[0]] for v in items] 
        backitems.sort(reverse=desc) #reverse=True
        #print(type(backitems))
        #backitems2=[[v[1],v[0]] for v in backitems] 
        return [backitems[i][1] for i in range(0,len(backitems))] 
    def re_set_all(self,group_num_new=0): #重置所有分组
        if group_num_new<3:
            print('group_num分组数太少，应该在4组以上')
            return 0
        else:
            self.group_num=group_num_new
            gp_map,gp_sql=self.group_sh()
            jobs_dict,group_max_rank_id=self.get_job_group()
            tb_sql_map=jobs_dict['sql_file']
            tb_dep_map=jobs_dict['depend']
            group_usedtime,sql_usedtime=self.get_group_usedtime()
            has_dep_tbs={}
            tb_gp_map={} #表分组 
            no_dep_tbs={} #特殊表提前执行
            for tb in tb_sql_map.keys():
                depd=eval(tb_dep_map[tb]) #依赖的表
                #tb_dep_map[tb]=depd
                new_depd=depd.copy() #将依赖表另存一份
                for tp in depd:  #去除依赖sdd的表
                    if tp[0:4] in confs.db_map.keys():
                        new_depd.remove(tp)
                    if tp in specia_list:
                        new_depd.remove(tp)
                if len(new_depd)>0:
                    has_dep_tbs[tb]=new_depd
                else :
                    if tb in specia_list:
                        no_dep_tbs[tb]=0 #特殊表加长时间以便使其放在首位
                    else:
                        tb_sql=tb_sql_map[tb]
                        if tb_sql in sql_usedtime.keys():  #有执行历史记录的以历史用时为准
                            no_dep_tbs[tb]=sql_usedtime[tb_sql]
                        else:
                            no_dep_tbs[tb]=99999
            no_dep_tbs=self.dict_sort_by_value(no_dep_tbs)              
            for i in range(len(no_dep_tbs)):
                tp=i%self.group_num+1
                gp_sql[tp].append(no_dep_tbs[i])
                tb_gp_map[no_dep_tbs[i]]=tp
            for tb in has_dep_tbs.keys():
                max_num=0
                for tp in has_dep_tbs[tb]:
                    if tp in tb_gp_map.keys():
                        tp_max_num=tb_gp_map[tp]
                        if tp_max_num>max_num:
                            max_num=tp_max_num
                    else :
                        print(tp,'依赖表没有加入配置')
                        return 0
                if max_num>0:
                    if tb in tb_gp_map.keys():
                        print(tb,'已经存在')
                        return 0
                    else:
                        gp_sql[max_num].append(tb)
                        tb_gp_map[tb]=max_num
            etl_data=conn.meta('etl_data')
            sql="""insert into job_group_set_his(tb_name,sql_file,group_id,depend,rank_id,create_time,update_time,freq_type,del_flag,cmds,oper_time)
                    select tb_name,sql_file,group_id,depend,rank_id,create_time,update_time,freq_type,del_flag,cmds,CURRENT_TIMESTAMP() from job_group_set;
                    delete from job_group_set where freq_type='{0}';"""
            etl_data.execute(sql.format(self.frency))
            sql="insert into job_group_set(tb_name,sql_file,depend,freq_type,group_id,rank_id,cmds) VALUES('{0}','{1}','{2}','{3}',{4},{5},'{6}')"
            for tb in gp_sql.keys():
                tb_list=gp_sql[tb]
                for i in range(len(tb_list)):
                    etl_data.execute(sql.format(tb_list[i],tb_sql_map[tb_list[i]],pymysql.escape_string(str(tb_dep_map[tb_list[i]])),self.frency,tb,i,confs.hive_sh+tb_sql_map[tb_list[i]]))
            return 1
    
    def del_job(self,tb_name): # 删除作业
        jobs_dict,group_max_rank_id=self.get_job_group()
        tb_dep_map=jobs_dict['depend']
        tb_sql_map=jobs_dict['sql_file']
        if tb_name in tb_sql_map.keys():
            sql_file=tb_sql_map[tb_name]
            for tp in tb_dep_map.keys():
                if tb_name in tb_dep_map[tp]:
                    print(tp,'依赖',tb_name,'不能删除')
                    return 0
            sql="update job_group_set set del_flag=1 where sql_file='{0}' and freq_type='{1}';".format(sql_file,self.frency)
            etl_data=conn.meta('etl_data')
            etl_data.execute(sql)
            self.write_sh()
            return 1
        else:
            print(tb_name,'没有部署，无法删除')
            return 0
    def add_job(self,sql_file,tb_name,depd_list): # 新增作业
        group_usedtime,sql_usedtime=self.get_group_usedtime()
        jobs_dict,group_max_rank_id=self.get_job_group() # 已经配置好的
        #print(group_max_rank_id)
        tb_group_map=jobs_dict['group_id']
        tb_dep_map=jobs_dict['depend']
        if tb_name in tb_dep_map or sql_file in jobs_dict['sql_file'].keys():
            print(tb_name,'已经部署，不能不能重复部署')
            return 0
        else:
            new_depd=depd_list.copy() #将依赖表另存一份
            for tp in depd_list:  #去除依赖sdd的表
                if tp[0:4] in confs.db_map.keys():
                    new_depd.remove(tp)
                if tp in specia_list:
                    new_depd.remove(tp)
            if len(new_depd)>0: #有依赖
                dep_group={}
                for tb in new_depd:
                    if tb in tb_group_map.keys():
                        group_id=tb_group_map[tb]
                        dep_group[group_id]=group_usedtime[group_id]
                    else:
                        print(tb,'依赖表没有加入配置')
                        return 0
                group_id=self.dict_sort_by_value(dep_group)[0]    
                #rank_id=group_max_rank_id.loc[group_id-1,'max_rank_id']+1   
            else:   #无依赖
                group_id=self.dict_sort_by_value(group_usedtime)[0]
            rank_id=group_max_rank_id.loc[group_id-1,'max_rank_id']+1
            sql="insert into job_group_set(tb_name,sql_file,depend,freq_type,group_id,rank_id,cmds) VALUES('{0}','{1}','{2}','{3}',{4},{5},'{6}')"
            etl_data=conn.meta('etl_data')
            etl_data.execute(sql.format(tb_name,sql_file,pymysql.escape_string(str(depd_list)),self.frency,group_id,rank_id,confs.hive_sh+sql_file))
            return 1
    def write_sh(self,group_id=0): #指定groupid则只更新group_id的分组
        engine=conn.meta('etl_data')
        sshcon=ssh_con()
        ssh_uat=ssh_cmd(sshcon.ssh_uat)
        ssh_sc=ssh_cmd(sshcon.ssh_sc)
        sql_txt="""
                    SELECT group_id,sql_file,cmds
                    FROM  job_group_set where del_flag=0 and freq_type='{0}'
                    order by group_id,rank_id
                  """
        job_group=pd.read_sql(sql_txt.format(self.frency),engine)
        #if group_id<1 or group_id>self.group_num: 
        gp_map,gp_sql=self.group_sh() #将文件清空
        for i in gp_map.keys():
            filepath=confs.main_path_bin+gp_map[i]
            f=open(filepath, 'a',encoding='utf-8') #打开文件
            tp=list(job_group[job_group['group_id']==i]['cmds'])
            for sqls in tp:
                f.write(sqls)
                f.write("\n")
            f.close()
            ssh_uat.upload(filepath,confs.remote_path_bin+gp_map[i])
            ssh_sc.upload(filepath,confs.remote_path_bin+gp_map[i])
        ssh_uat.cmd_run(['chmod 755 -R /home/bigdata/bin /home/bigdata/sql /home/bigdata/cfg'])
        ssh_sc.cmd_run(['chmod 755 -R /home/bigdata/bin /home/bigdata/sql /home/bigdata/cfg'])
        ssh_uat.close()
        ssh_sc.close()
        return 1
    def is_utf8_file(self,filepath): #是否utf8,但是有bom和无bom无法区别
        try:
            f=open(filepath,'r',encoding='utf-8')
            f.read()
            f.close()
            return 1
        except Exception as e:
            f.close()
            return 0
    def read_deploy(self):
        #判断表存在、文件存在、依赖、是否sqoop判断
        filepath=confs.main_path+'deploy_file.properties'
        tb_list=set()
        with open(filepath, 'r') as f:
            lines = f.readlines() #读取所有行
            for line in lines:
                tp=line.replace('.sql','').replace('.sh','').replace('.properties','').replace(',','')
                if len(tp)>4:
                    tb_list.add(tp.strip())
        f.close()
        #print(tb_list)
        return list(tb_list)
    
    def check_deploy(self,tb): #文件检测
        if not os.path.exists(confs.main_path+'sql/'+tb+'.sql'):
            print(tb+'.sql','文件不存在')
            return 0,'null','null'
        else:
            sql_tb,sql_tb_cn,sql_author=self.read_sqlfile(tb+'.sql')
        if self.is_utf8_file(confs.main_path+'sql/'+tb+'.sql')==0:
            print(tb+'.sql','不是UTF-8格式')
            return 0,'null','null'
        if  os.path.exists(confs.main_path+'cfg/'+tb+'.properties'):
            if self.is_utf8_file(confs.main_path+'cfg/'+tb+'.properties')==0:
                print(tb+'.properties','不是UTF-8格式')
                return 0,'null','null'
            keys=''
            values=[]
            with open(confs.main_path+'cfg/'+tb+'.properties', 'r',encoding='utf-8') as f:  #打开文件
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
            #print(file_rs['[results]'])
            if len(file_rs['[results]'])==1:
                tar_tb=file_rs['[results]'][0]
                if conn.hive_tb_exists(tar_tb)==0:
                    print('hive不存目标表：',tar_tb)
                    #return 0,'null'
                if not tar_tb==sql_tb:
                    print('properties文件【result】表名称和目标表不一致',tar_tb,sql_tb)
                    return 0,'null','null'
            else:
                print('properties文件【result】没有指定生成结果文件名或者指定多个结果')
                return 0,'null'
            if len(file_rs['[dependence]'])>0:
                for tp in file_rs['[dependence]']:
                    #检查依赖表的配置情况
                    if conn.etl_set_exists(tp)==0:
                        print(tp,'依赖配置没有加入调度')
                        return 0,'null','null'
                    #print(tp)
            else:
                print('properties文件【dependcy】没有指定依赖文件')     
                return 0,'null','null'
            if len(file_rs['[properties]'])>0:
                for tp in file_rs['[properties]']:
                    if 'dev' in tp:
                        print('dev不应该出现在配置文件的[properties]中')
                        return 0,'null','null'
            else:
                print('properties文件【properties】配置正确')     
                return 0,'null','null'
        else:
            print(tb+'.properties','文件不存在')
            return 0,'null','null'
        return 1,tar_tb,file_rs['[dependence]']
    
    def read_sqlfile(self,file_name):
        filepath=confs.main_path_sql+file_name
        target_tb_cn=''
        target_tb=''
        create_by=''
        if os.path.exists(filepath):
                try:
                    #print(files)
                    if  os.path.isfile(filepath) and file_name.endswith('.sql'):
                        with open(filepath, 'r',encoding='utf-8') as f:  #打开文件
                            lines = f.readlines() #读取所有行
                            target_tb_cn=''
                            create_by=''
                            for line in lines[0:15]:
                                line=line.strip()
                                if 'see:' in line:
                                    target_tb_cn=line[line.find(':')+1:].strip()
                                if '}.' in line:
                                    target_tb=line[line.find('}.')+2:].strip()
                                    target_tb=target_tb.replace('(','').replace(' ','')
                                if 'author:' in line:
                                    create_by=line[line.find(':')+1:].strip()
                        #tables_list.loc[nums]=[files,target_tb_cn,create_by]
                except Exception as e:
                        #print('str(Exception):\t', str(Exception))
                        print (file_name,'\t error :\t\t',str(e))     
                return target_tb,target_tb_cn,create_by
        else:
            print(file_name,'文件不存在')   
            return target_tb,target_tb_cn,create_by      
    def sdd_table(self,db,tb_list):#uat和生产环境同步建SDD表
        sshcon=ssh_con()
        ssh=ssh_cmd(sshcon.ssh_uat)
        is_success=ssh.hive_ddl(db,tb_list)
        if is_success>0:
            ssh=ssh_cmd(sshcon.ssh_sc)
            ssh.hive_ddl(db,tb_list)
        ssh.close()      
    def append_sh(self,filepath,tar_cmd):
        if  filepath.endswith('.sh') :
               if not os.path.exists(filepath):
                    #print(confs.main_path+'bin/template.sh',os.path.exists(confs.main_path+'bin/template.sh'))
                    open(filepath, "wb").write(open(confs.main_path+'bin/template.sh', "rb").read())
               with open(filepath, 'r',encoding='utf-8') as fr:
                    for tp in fr.readlines():
                        if tar_cmd in tp:
                            print('分组文件已经添加shell命令不能重复配置')
                            return 1
                    fr.close()
               with open(filepath, 'a',encoding='utf-8') as f:  #打开文件
                    f.write(tar_cmd)
                    f.write("\n")
                    f.close()
                    
               with open(filepath, 'r',encoding='utf-8') as fr:
                    for tp in fr.readlines():
                        if tar_cmd.endswith('.sql'):
                            if len(tp)>160:
                                print(filepath,'文件配置错位')
                                return 0
                        else:
                            if len(tp)>100:
                                print(filepath,'文件配置错位')
                                return 0
                    fr.close()
        else:
            print('无效文件，必须是.sh文件')  
        return 1  
    def auto_deploy(self,tar_ssh='ssh_uat'): 
        tb_list=self.read_deploy()
        print(tb_list)
        sshcon=ssh_con()
        #ssh=ssh_cmd(sshcon.ssh_uat)
        if tar_ssh=='ssh_sc':
            self.ssh=ssh_cmd(sshcon.ssh_sc)
        ssh=self.ssh
        for tb in tb_list:
            heads=tb[0:4]
            if heads in confs.db_map.keys():
                print('\n  sqoop同步配置:',tb)
                tp_tb=tb[5:]
                tar_cmd=heads+' '+tp_tb+' auto'
                tb_size=conn.sljr_tb_size(db=heads,tb=tp_tb)
                if conn.etl_set_exists(tb)>0:
                    print(tb,'目标表已经加入了调度，如果需要重新调度请手动修改')
                    break
                if tb_size<0:
                    print(tp_tb,'表不存在不能同步，或者检查表名')
                    break
                if tb_size>10000000:
                    print(tp_tb,'大于1千万需要增量同步:',tb_size)
                    tar_cmd=tar_cmd+' inc'
                if conn.hive_tb_exists(tb)==0:
                    self.sdd_table(db=heads,tb_list=[tp_tb]) #同步表结构
                group_sh=confs.local_path+'bin/sqoop_'+heads+'.sh'
                tar_cmd=confs.sqoop_sh+tar_cmd
                if self.append_sh(group_sh,tar_cmd)>0:  
                    if ssh.cmd_run([tar_cmd])>0:
                        ssh.upload(group_sh,confs.remote_path+'bin/sqoop_'+heads+'.sh')
                else:
                    print(heads,'shell文件配置错位')
                    break
            else:
                #hive sql配置
                print('\n  hive sql同步配置检测:',tb)
                flag,tar_tb,depd_list=self.check_deploy(tb)
                if flag==0:
                    print('\033[1;37;45m ERROR:',tb,'  配置文件检查错误        \033[0m')
                    break
                else:
                    print('检测通过：',tb)
                    ssh.upload(confs.main_path+'cfg/'+tb+'.properties',confs.remote_path+'cfg/'+tb+'.properties')
                    ssh.upload(confs.main_path+'sql/'+tb+'.sql',confs.remote_path+'sql/'+tb+'.sql')
                    #ssh.upload(confs.main_path+'bin/'+tb+'.sh',confs.remote_path+'bin/'+tb+'.sh')
                    tar_cmd=confs.hive_sh+tb+'.sql'
                    #print('执行数据同步完成')
                    if ssh.cmd_run([tar_cmd])>0:
                        if self.add_job(tb+'.sql',tar_tb,depd_list)>0:
                            self.write_sh()
                    else:
                        #self.write_sh()
                        print('\033[1;37;45m ERROR:',tb,' sql执行错误，请修改        \033[0m')

        ssh.cmd_run(['chmod 755 -R /home/bigdata/bin /home/bigdata/sql /home/bigdata/cfg'])
        ssh.close()
    def run_sql(self,tb,tar_ssh='ssh_uat'):  
        sshcon=ssh_con()
        ssh=ssh_cmd(sshcon.ssh_uat)
        if tar_ssh=='ssh_sc':
            ssh=ssh_cmd(sshcon.ssh_sc)
        flag,tar_tb,depd_list=self.check_deploy(tb)
        if flag==0:
            print('\033[1;37;45m ERROR:',tb,'  配置文件检查错误        \033[0m')
        else:
           print('检测通过：',tb)
           ssh.upload(confs.main_path+'cfg/'+tb+'.properties',confs.remote_path+'cfg/'+tb+'.properties')
           ssh.upload(confs.main_path+'sql/'+tb+'.sql',confs.remote_path+'sql/'+tb+'.sql')
           tar_cmd=confs.hive_sh+tb+'.sql'
           #print('执行数据同步完成')
           if ssh.cmd_run([tar_cmd])>0:
               print('执行成功')
           else:
               print('\033[1;37;45m ERROR:',tb,' sql执行错误，请修改        \033[0m')
        ssh.close()        
if __name__ == '__main__':  
    auto=auto_schedule()
    auto.auto_deploy(tar_ssh='ssh_sc')
    #auto.run_sql(tb='dim_cust_base_info_daily',tar_ssh='ssh_sc')
    