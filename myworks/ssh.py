import os
import time
import datetime
import paramiko
from etlpy.cons import conn as cons
root_path='F:/home/bigdata/python' 
key_file=root_path+'/cfg/id_rsa' #秘钥路径
log_path=root_path+'/log/' #日志路径
tar_path='/home/bigdata/python/upload' #目标路径
bakup_path='/home/bigdata/python/bakup/' #远程备份目录
today=time.strftime('%Y%m%d',time.localtime())

class Logger(object):
    '''模拟日志类。方便单元测试。'''
    def __init__(self):
        self.info = self.error = self.critical = self.debug
    def debug(self, msg):
        print("LOGGER:"+msg)
        
class SSH(object):
    def __init__(self):
        try:
            ssh_uat = paramiko.SSHClient()
            ssh_sc = paramiko.SSHClient()
            ssh_uat.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_sc.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            if os.path.exists(key_file):
                ssh_sc.connect('192.168.190.11',22,'bigdata',timeout=5,key_filename=key_file)
                ssh_uat.connect('192.168.188.80',22,'bigdata',timeout=5,key_filename=key_file+'_test')
            else:
                ssh_sc.connect('192.168.190.11',22,'bigdata',timeout=5)
                ssh_uat.connect('192.168.188.80',22,'bigdata',timeout=5)
        except Exception as e :
            print ('\n%s\t 连接错误:\t'%(ip),str(e))
        self.ssh_sc=ssh_sc
        self.ssh_uat=ssh_uat 
        
    def get_ssh(ip='192.168.190.11',username ="bigdata",passwd='password'): #ssh远程连接
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            if os.path.exists(key_file):
                ssh.connect(ip,22,username,passwd,timeout=5,key_filename=key_file)
            else:
                ssh.connect(ip,22,username,passwd,timeout=5)
        except Exception as e :
            print ('\n%s\t 连接错误:\t'%(ip),str(e))
        return ssh 

class SSH_cmd(object):
    def __init__(self,ssh):
        self.ssh=ssh
        self.sftp=ssh.open_sftp()
        self.logger = Logger()
    def file_exist(self,path): #远程文件是否存在
        try:
            self.sftp.stat(path)
            return 1
        except Exception as e:
            return 0
    def file_bakup(self,path): #远程文件备份
        now_time=time.strftime('%Y%m%d_%H%M%S',time.localtime())
        path=path.replace('\\','/')
        if path.endswith('/'):
            path=path[:-1]
        try:
            cmd='mv '+path+' /home/bigdata/python/bakup/'+now_time+'_'+os.path.split(path)[1]
            #print(cmd)
            stdin, stdout, stderr = self.ssh.exec_command(cmd)
            if len(stderr.readlines())>0:
                print('备份失败：',cmd)
                return 0
            return 1
        except Exception as e:
            print(str(e))
            return 0  
    def save_file(self,filename,txts):
        file_object = open(filename, 'w')
        for x in txts:
            file_object.write(x)
        file_object.close()
    def upload(self,local_dir,remote_dir): #文件或者文件夹上传
        local_dir=local_dir.replace('\\','/')
        if 'bigdata/python/upload'  in remote_dir or 'bigdata/python/hive'  in remote_dir:
            try:  
                sftp=self.sftp 
                if os.path.isdir(local_dir): #同步文件夹
                    if self.file_exist(remote_dir)==0: #判断目录是否存在
                        sftp.mkdir(remote_dir)
                    else:
                        if self.ssh_bakup(remote_dir)>0:
                            self.file_exist(remote_dir) #需要对目录信息刷新
                            sftp.mkdir(remote_dir)
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
                                sftp.mkdir(remote_path)  
                            except Exception as e:  
                                print("创建目录错误",remote_path,str(e)) 
                           # 以下同步文件    
                        for filespath in files:  
                            local_file = os.path.join(root,filespath).replace('\\','/')
                            #print(11,'[%s][%s][%s][%s]' % (root,filespath,local_file,local_dir))  
                            a = local_file.replace(local_dir,'').replace('\\','/').lstrip('/')  
                            remote_file = os.path.join(remote_dir,a).replace('\\','/') 
                            #print(22,remote_file,local_file)  
                            try: 
                                print('上传：',local_file)
                                sftp.put(local_file,remote_file)  
                            except Exception as e:
                                up_dir=os.path.split(remote_file)[0]
                                print("上级目录不存,创建上级目录:",up_dir)
                                sftp.mkdir(up_dir)  
                                sftp.put(local_file,remote_file)  
                                #print("66 upload %s to remote %s" % (local_file,remote_file))  
                else:
                    if self.file_exist(remote_dir)>0: #判断目录是否存在
                        if self.file_bakup(remote_dir)>0:
                                self.file_exist(remote_dir) #需要对目录信息刷新
                    try:  
                        print('上传：',local_dir)
                        sftp.put(local_dir,remote_dir)  
                    except Exception as e:
                        up_dir=os.path.split(remote_file)[0]
                        print("上级目录不存,创建上级目录:",up_dir)
                        sftp.mkdir(up_dir)  
                        sftp.put(local_dir,remote_dir)             
                print('上传完成')
                return 1
            except Exception as e:  
                print('上传错误提示：',str(e))  
                return 0
        else:
            print('ERROR: 只能对/home/bigdata/python/upload进行操作')
            return 0
    
    def cmd_run(self,cmd,if_print=1): #执行命令列表，if_print=1表示打印日志
        logs=time.strftime('%Y%m%d_%H%M%S',time.localtime())
        if os.path.exists(log_path):
            logs=log_path+logs
        else:
            logs=log_path[2:]+logs
        try:
            max_num=len(cmd)
            for m in range(max_num):
                begin_time=time.time()
                print('开始执行第',m+1,'条_共',max_num,'条_完成比例:',round(100*(m+1)/max_num,2))
                stdin, stdout, stderr = self.ssh.exec_command(cmd[m]) 
                out=stderr.readlines()
                channel = stdout.channel
                status = channel.recv_exit_status()
                #print(status,'stayus')
                if status>0:
                    print('命令执行错误：')
                    for o in out:
                        print(o)
                    self.save_file(logs+'_'+str(m).zfill(3)+'.log',out)
                else:
                    out = stdout.readlines()
                    if if_print>0:
                        for o in out:
                            print(o)
                print('用时：',round(time.time()-begin_time,2))
                self.save_file(logs+'_'+str(m).zfill(3)+'.log',out)
            print ('\tssh 执行完成 \n')
        except Exception as e :
            print ('\n\t 连接错误:\t',str(e))
        
    def hive_ddl(self,db,tb_list,sqlfile='/home/bigdata/python/hive/ddl.sql'):
        try:
            if cons.get_hive_dml(db,tb_list)>0:
                if self.upload('F:'+sqlfile,sqlfile)>0:
                    cmd = ["hive -f "+sqlfile]#你要执行的命令列表
                    self.cmd_run(cmd,if_print=1)
        except Exception as e :
            print ('\n\t 创建表错误:\t',str(e))
    def close(self):
        self.ssh.close()
        
    def sqoop_import(src_tb,tar_tb):
        cmd="""sqoop import --connect jdbc:mysql://rr-uf6j7j02i75dxe651o.mysql.rds.aliyuncs.com:3306/sljr_risk  
               --username dc_select  
               --password 'Ksdj@s2^dh'  
               --table"""
           

    

    
if __name__=='__main__':
     ssh=SSH()
     cmd_list=['pad','env','java']
     ssh_cmd=SSH_cmd(ssh.ssh_uat)
     ssh_cmd.file_exist('/home/bigdata/python/hive/') #文件是否存在
     #ssh_cmd.file_bakup('/home/bigdata/python/hive/')
     ssh_cmd.upload('F:/python/hive/ddl.sql','/home/bigdata/python/hive/ddl.sql')
     ssh_cmd.cmd_run(cmd_list,if_print=1)
  