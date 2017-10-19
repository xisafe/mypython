import os
import paramiko
import threading
key_file='f:/id_rsa'
def ssh2(ip,username,passwd,cmd):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if os.path.exists(key_file):
            ssh.connect(ip,22,username,passwd,timeout=5,key_filename=key_file)
        else:
            ssh.connect(ip,22,username,passwd,timeout=5)
        for m in cmd:
            stdin, stdout, stderr = ssh.exec_command(m)
            stdin.write("Y")   #简单交互，输入 ‘Y’ 
            out = stdout.readlines()
            #屏幕输出
            for o in out:
                print(o)
        print ('%s\tOK\n'%(ip))
        ssh.close()
    except Exception as e :
        print ('%s\t 连接错误:\t'%(ip),str(e))
def ssh(ip,username,cmd):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if os.path.exists(key_file):
            ssh.connect(ip,22,username,timeout=5,key_filename=key_file)
        else:
            ssh.connect(ip,22,username,timeout=5)
        for m in cmd:
            stdin, stdout, stderr = ssh.exec_command(m)
            stdin.write("Y")   #简单交互，输入 ‘Y’ 
            out = stdout.readlines()
            #屏幕输出
            print(ip,'shell命令执行：')
            for o in out:
                print(o)
        print ('%s\t 执行完成 \n'%(ip))
        ssh.close()
    except Exception as e :
        print ('%s\t 连接错误:\t'%(ip),str(e))
if __name__=='__main__':
    cmd = [ 'mkdir /']#你要执行的命令列表
    ip='192.168.190.11'
    username ="bigdata"  #用户名
    passwd = "Sh@nlin1234"    #密码
    threads = []   #多线程
    print("Begin......")
    #ssh_cmd(ip, passwd, cmd)
    ssh2('192.168.190.11',username,passwd,cmd)
    #for i in range(1,254):
        #ip = '192.168.1.'+str(i)
        #a=threading.Thread(target=ssh2,args=(ip,username,passwd,cmd))
        #a.start()