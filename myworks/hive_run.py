from ssh import SSH_cmd as ssh_cmd
from ssh import SSH as ssh_con
from cons import conn
from confs import main_path as root_path
import confs
import os

def append_sh(filepath,tar_cmd):
    sqoop_sh='sh /home/bigdata/bin/sqoop_handler_v1.1.ksh '
    hive_sh='/home/bigdata/bin/hive_sql_handler_v1.0.ksh /home/bigdata/sql/'
    if tar_cmd.endswith('.sql'):
        tar_cmd=hive_sh+tar_cmd
    else:
        #sdd_db=tar_cmd[0:4]
        tar_cmd=sqoop_sh+tar_cmd
    if  filepath.endswith('.sh') :
           if not os.path.exists(filepath):
                #print(confs.main_path+'bin/template.sh',os.path.exists(confs.main_path+'bin/template.sh'))
                open(filepath, "wb").write(open(confs.main_path+'bin/template.sh', "rb").read())
           with open(filepath, 'r',encoding='utf-8') as fr:
                for tp in fr.readlines():
                    if tar_cmd in tp:
                        print('分组文件已经配置，不能重复配置')
                        return 0
                fr.close()
           with open(filepath, 'a') as f:  #打开文件
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

def read_deploy():
    #判断表存在、文件存在、依赖、是否sqoop判断
    filepath=root_path+'deploy_file.properties'
    tb_list=set()
    with open(filepath, 'r') as f:
        lines = f.readlines() #读取所有行
        for line in lines:
            tp=line.replace('.sql','').replace('.sh','').replace('.properties','')
            if len(tp)>4:
                tb_list.add(tp.strip())
    f.close()
    #print(tb_list)
    return list(tb_list)

def check_deploy(tb):
    if not os.path.exists(root_path+'bin/'+tb+'.sh'):
       print(tb+'.sh','文件不存在')
       return 0,'null'
    if not os.path.exists(root_path+'sql/'+tb+'.sql'):
       print(tb+'.sql','文件不存在')
       return 0,'null'
    if  os.path.exists(root_path+'cfg/'+tb+'.properties'):
        keys=''
        values=[]
        with open(root_path+'cfg/'+tb+'.properties', 'r') as f:  #打开文件
            lines = f.readlines() #读取所有行
            file_rs={}
            for line in lines:
                line=line.strip()
                #if '=dev' in line or '= dev':
                 #   print('dev不应该出现在配置文件中')
                 #   return 0,'null'
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
                return 0,'null'
        else:
            print('properties文件【result】没有指定生成结果文件名或者指定多个结果')
            return 0,'null'
        if len(file_rs['[dependence]'])>0:
            for tp in file_rs['[dependence]']:
                #检查依赖表的配置情况
                if conn.etl_set_exists(tp)==0:
                    print(tp,'依赖配置没有加入调度')
                    return 0,'null'
                #print(tp)
        else:
            print('properties文件【dependcy】没有指定依赖文件')     
            return 0,'null'
        if len(file_rs['[properties]'])>0:
            for tp in file_rs['[properties]']:
                if 'dev' in tp:
                    print('dev不应该出现在配置文件的[properties]中')
                    return 0,'null'
        else:
            print('properties文件【properties】配置正确')     
            return 0,'null'
    else:
        print(tb+'.properties','文件不存在')
        return 0,'null'
    return 1,tar_tb

def sqoop_tp():#大表同步
    cmd="""sqoop import --connect jdbc:mysql://rr-uf6j7j02i75dxe651o.mysql.rds.aliyuncs.com:3306/sljr_risk  --username dc_select  --password 'Ksdj@s2^dh'  --table user_contacts_converse --columns 'id,user_id,type,conv_time,contacts_mobile,contacts_name,remark,create_time,update_time,status,talk_time' --where "id>={0} and id<{1}" --fields-terminated-by '\\001' --direct -m 4 --delete-target-dir --hive-import --hive-overwrite --hive-table dev.fkxt_user_contacts_converse  --null-string '\\\\N' --null-non-string '\\\\N' \n"""    
    i=0
    into_hive="""hive -e 'insert into sdd.fkxt_user_contacts_converse partition(dt='20171014') 
            select current_timestamp() load_data_time,id,user_id,type,conv_time,contacts_mobile,contacts_name,remark,create_time,update_time,status,talk_time
            from dev.fkxt_user_contacts_converse' """
    cmd_list=[]
    while i<10000000:
        j=i+1000000
        cmd_list.append(cmd.format(i,j))
        cmd_list.append(into_hive)
        i=j
    return cmd_list
def sdd_table(db,tb_list):#uat和生产环境同步建SDD表
    sshcon=ssh_con()
    ssh=ssh_cmd(sshcon.ssh_uat)
    is_success=ssh.hive_ddl(db,tb_list)
    if is_success>0:
        ssh=ssh_cmd(sshcon.ssh_sc)
        ssh.hive_ddl(db,tb_list)
    ssh.close()
def auto_deploy(etl_group,tar_ssh='ssh_uat'): 
    tb_list=read_deploy()
    sshcon=ssh_con()
    ssh=ssh_cmd(sshcon.ssh_uat)
    if tar_ssh=='ssh_sc':
        ssh=ssh_cmd(sshcon.ssh_sc)
    for tb in tb_list:
        heads=tb[0:4]
        if heads in confs.db_map.keys():
            print('sqoop同步配置:',tb)
            tp_tb=tb[5:]
            tar_cmd=db+' '+tb+' auto'
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
                sdd_table(db=heads,tb_list=[tp_tb]) #同步表结构
            group_sh=confs.local_path+'bin/sqoop_'+heads+'.sh'
            if append_sh(group_sh,tar_cmd)>0:
                ssh.upload(group_sh,confs.remote_path+'bin/sqoop_'+heads+'.sh')
            else:
                print(heads,'shell文件配置错位')
                break
        else:
            #hive sql配置
            print('hive sql同步配置检测:',tb)
            flag,tar_tb=check_deploy(tb)
            if flag==0:
                print(tb,'配置文件检查错误')
                break
            else:
                print('检测通过：',tb)
                if tb in etl_group.keys():
                    if conn.etl_set_exists(tar_tb)>0:
                        print(tar_tb,'目标表已经加入了调度，如果需要重新调度请手动修改')
                    else:
                        group_sh=confs.local_path+'bin/'+etl_group[tb]
                        if append_sh(group_sh,tb+'.sql')>0:
                            ssh.upload(group_sh,confs.remote_path+'bin/'+etl_group[tb])
                        else:
                            print(etl_group[tb],'shell文件配置错位')
                            break
                    ssh.upload(confs.main_path+'cfg/'+tb+'.properties',confs.remote_path+'cfg/'+tb+'.properties')
                    ssh.upload(confs.main_path+'sql/'+tb+'.sql',confs.remote_path+'sql/'+tb+'.sql')
                    ssh.upload(confs.main_path+'bin/'+tb+'.sh',confs.remote_path+'bin/'+tb+'.sh')
                    ssh.cmd_run(['chmod 755 -R /home/bigdata/bin'])
                else:
                    print('脚本没有指定分组调度')
                    break
    ssh.close()            
    
if __name__=='__main__':
    cmd = [ 'sh /home/bigdata/bin/sqoop_handler_v1.1.ksh jrxj user_profession auto' ]#你要执行的命令列表
    db='jrxj'
    tb_list=['user_profession'
            ]
    etl_group={'dim_cust_device_op_dtl_daily':'subject_cust_daily2.sh',
               'dim_channel':'subject_cust_daily2.sh'
               }
    auto_deploy(etl_group,tar_ssh='ssh_uat')
    