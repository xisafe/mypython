import os
import time
import datetime
import paramiko
from etlpy.cons import conn as cons
root_path='F:/home/bigdata/python'
key_file=root_path+'/cfg/id_rsa'
log_path=root_path+'/log/'
tar_path='/home/bigdata/python/upload'
today=time.strftime('%Y%m%d',time.localtime())

def get_ssh(ip='192.168.190.11',username ="bigdata"): #ssh远程连接
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if os.path.exists(key_file):
            ssh.connect(ip,22,username,timeout=5,key_filename=key_file)
        else:
            ssh.connect(ip,22,username,timeout=5)
    except Exception as e :
        print ('\n%s\t 连接错误:\t'%(ip),str(e))
    return ssh 

def get_ssh_test(ip='192.168.188.80',username ="bigdata"): #测试环境ssh远程连接
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if os.path.exists(key_file):
            ssh.connect(ip,22,username,timeout=5,key_filename=key_file+'_test')
        else:
            ssh.connect(ip,22,username,timeout=5)
    except Exception as e :
        print ('\n%s\t 连接错误:\t'%(ip),str(e))
    return ssh 
ssh=get_ssh()
ssh_test=get_ssh_test()
def ssh_exist(path,sftp): #远程文件是否存在
    try:
        sftp.stat(path)
        return 1
    except Exception as e:
        return 0
def ssh_backup(path,ssh): #远程文件备份
    now_time=time.strftime('%Y%m%d_%H%M%S',time.localtime())
    try:
        #print(os.path.split(path)[1])
        cmd='mv '+path+' /home/bigdata/python/bakup/'+now_time+'_'+os.path.split(path)[1]
        #print(cmd)
        ssh.exec_command(cmd) 
        return 1
    except Exception as e:
        print(str(e))
        return 0  
    
def upload(local_dir,remote_dir):
    if 'bigdata/python/upload'  in remote_dir or 'bigdata/python/hive'  in remote_dir:
        try:  
            sftp=ssh.open_sftp()
            sftp_test=ssh_test.open_sftp()
            if os.path.isdir(local_dir): #同步文件夹
                if ssh_exist(remote_dir,sftp)==0: #判断目录是否存在
                    sftp.mkdir(remote_dir)
                    sftp_test.mkdir(remote_dir)
                else:
                    if ssh_backup(remote_dir,ssh)>0:
                        ssh_exist(remote_dir,sftp) #需要对目录信息刷新
                        sftp.mkdir(remote_dir)
                        ssh_exist(remote_dir,sftp_test) #需要对目录信息刷新
                        sftp_test.mkdir(remote_dir)
                for root,dirs,files in os.walk(local_dir):  
                    #print('文件 [%s][%s][%s]' % (root,dirs,files))
                    #同步目录
                    for name in dirs:
                        local_path = os.path.join(root,name).replace('\\','/')  
                        #print("本地目录",local_path,local_dir)  
                        a = local_path.replace(local_dir,'').replace('\\','')  
                        #print("创建目录",a)  #print("远程上级目录",remote_dir)  
                        remote_path = remote_dir+a   #print("远程路径",remote_path)  
                        try:  
                            #print(44,"mkdir path %s" % remote_path)
                            sftp.mkdir(remote_path)  
                        except Exception as e:  
                            print("创建目录错误",remote_path,str(e)) 
                       # 以下同步文件    
                    for filespath in files:  
                        local_file = os.path.join(root,filespath).replace('\\','/')
                        #print(11,'[%s][%s][%s][%s]' % (root,filespath,local_file,local_dir))  
                        a = local_file.replace(local_dir,'').replace('\\','/').lstrip('/')  
                        #print('01',a,'[%s]' % remote_dir)  
                        remote_file = os.path.join(remote_dir,a).replace('\\','/') 
                        #print(22,remote_file,local_file)  
                        try:  
                            sftp.put(local_file,remote_file)  
                        except Exception as e:
                            up_dir=os.path.split(remote_file)[0]
                            print("上级目录不存,创建上级目录:",up_dir)
                            sftp.mkdir(up_dir)  
                            sftp.put(local_file,remote_file)  
                            #print("66 upload %s to remote %s" % (local_file,remote_file))  
            else:
                if ssh_backup(remote_dir,ssh)>0:
                        ssh_exist(remote_dir,sftp) #需要对目录信息刷新
                        try:  
                            sftp.put(local_dir,remote_dir)  
                        except Exception as e:
                            up_dir=os.path.split(remote_file)[0]
                            print("上级目录不存,创建上级目录:",up_dir)
                            sftp.mkdir(up_dir)  
                            sftp.put(local_dir,remote_dir) 
                            
            print('文件上传成功： %s ' % datetime.datetime.now())  
            return 1
        except Exception as e:  
            print('上传错误提示：',str(e))  
            return 0
    else:
        print('ERROR: 只能对/home/bigdata/python/upload进行操作')
        return 0
    
def sh_run(cmd,ssh=ssh,if_print=0):
    logs=time.strftime('%Y%m%d_%H%M%S',time.localtime())
    if os.path.exists(log_path):
        logs=log_path+logs
    else:
        logs='/home/bigdata/python/log/'+logs
    try:
        max_num=len(cmd)
        for m in range(max_num):
            begin_time=time.time()
            print('开始执行第',m,'条_共',max_num,'条_完成比例:',round(100*(m+1)/max_num,2))
            stdin, stdout, stderr = ssh.exec_command(cmd[m]) 
            out=stderr.readlines()
            if len(out)>0:
                print('命令执行错误：')
                if if_print>0:
                    for o in out:
                        print(o)
                #print(cmd[m])
                save_file(logs+'_'+str(m).zfill(3)+'.log',out)
            else:
                out = stdout.readlines()
                if if_print>0:
                    for o in out:
                        print(o)
            print('用时：',round(time.time()-begin_time,2))
            save_file(logs+'_'+str(m).zfill(3)+'.log',out)
        print ('\tssh 执行完成 \n')
    except Exception as e :
        print ('\n\t 连接错误:\t',str(e))
        
def hive_ddl(db,tb_list,sqlfile='/home/bigdata/python/hive/ddl.sql'):
    try:
        cons.get_hive_dml(db,tb_list)
        if upload('F:'+sqlfile,sqlfile,ssh)>0:
            cmd = ["hive -f "+sqlfile]#你要执行的命令列表
            sh_run(cmd,ssh,if_print=1)
    except Exception as e :
        print ('\n\t 创建表错误:\t',str(e))

def sqoop_import(src_tb,tar_tb):
    cmd="""sqoop import --connect jdbc:mysql://rr-uf6j7j02i75dxe651o.mysql.rds.aliyuncs.com:3306/sljr_risk  
           --username dc_select  
           --password 'Ksdj@s2^dh'  
           --table"""
           
def save_file(filename,txts):
    file_object = open(filename, 'w')
    for x in txts:
        file_object.write(x)
    file_object.close()
    
def sqoop_tp():
    cmd="""sqoop import --connect jdbc:mysql://rr-uf6j7j02i75dxe651o.mysql.rds.aliyuncs.com:3306/sljr_risk  --username dc_select  --password 'Ksdj@s2^dh'  --table user_contacts_converse --columns 'id,user_id,type,conv_time,contacts_mobile,contacts_name,remark,create_time,update_time,status,talk_time' --where "id>={0} and id<{1}" --fields-terminated-by '\\001' --direct -m 4 --delete-target-dir --hive-import --hive-overwrite --hive-table dev.fkxt_user_contacts_converse  --null-string '\\\\N' --null-non-string '\\\\N' \n"""    
    i=0
    into_hive="""hive -e 'insert into sdd.fkxt_user_contacts_converse partition(dt='20171014') 
            select current_timestamp() load_data_time,id,user_id,type,conv_time,contacts_mobile,contacts_name,remark,create_time,update_time,status,talk_time
            from dev.fkxt_user_contacts_converse' """
    cmd_list=[]
    while i<60000000:
        j=i+10000000
        cmd_list.append(cmd.format(i,j))
        cmd_list.append(into_hive)
        i=j
     
    return cmd_list
if __name__=='__main__':
    db='fkxt'
    tb_list=['approve_borrower_info','approve_credit_card','approve_credit_card_detail']
    #hive_file(db,tb_list)
    #cons.get_hive_dml(db,tb_list)
    ssh=get_ssh_test()
    #if upload('F:/home/bigdata/python/hive/ddl.sql','/home/bigdata/python/hive/ddl.sql',ssh)>0:
     #   cmd = [ "hive -f '/home/bigdata/python/hive/ddl.sql'" ]#你要执行的命令列表
     #   sh_run(cmd,ssh,if_print=1)
    #ssh_backup(tar_path+'hive/dim_cust_contact_book_daily.sql',ssh)
    upload('F:/home/bigdata/python/meta_satic.py',tar_path+'/test2.py')
    ssh.close()
    ssh_test.close()
    
  