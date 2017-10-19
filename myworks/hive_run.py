from ssh import SSH_cmd as ssh_cmd
from ssh import SSH as ssh_con
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
if __name__=='__main__':
    cmd = [ 'echo "hehf"','javac' ]#你要执行的命令列表
    cmd=sqoop_tp()
    db='zhxt'
    tb_list=['user_account',
             'user_account_org',
                'user_status',
                'user_account_log'
            ]
    ssh_con=ssh_con()
    #ssh_sc=ssh_cmd(ssh_con.ssh_sc)
    ssh_uat=ssh_cmd(ssh_con.ssh_uat)
    ssh=ssh_con.ssh_uat
    stdin, stdout, stderr=ssh.exec_command('pwd')
    #ssh_uat.hive_ddl(db,tb_list)
    ssh_uat.cmd_run(cmd)
    #/home/bigdata/bin/auto_hive_deploy.sh
    #cmd=['java','env','/home/bigdata/bin/auto_hive_deploy.sh']
    #ssh_cmd.sh_run(cmd,if_print=1)
    ssh_uat.close()