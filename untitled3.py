import os
import time
import datetime
import paramiko
key_file='f:/ssh/id_rsa'
log_path='f:/ssh/log'
def sftp_upload_file(server_path, local_path):
    ip='192.168.190.11'
    username ="bigdata"  #用户名
    try:
        t = paramiko.Transport((ip, 22))
        t.connect(username=username)
        sftp = paramiko.SFTPClient.from_transport(t)
        print('upload file start %s ' % datetime.datetime.now())
        sftp.put(local_path, server_path,password='Sh@nlin1234')
        print('77,upload file success %s ' % datetime.datetime.now())
        t.close()
    except Exception as e:
        print(e)
        

        
if __name__=='__main__':
    #cmd = [ 'echo "hehf"' ]#你要执行的命令列表
    sftp_upload_file('E:/collection_hive.sql', '/home/bigdata/python/temp/collection_hive.sql')
  