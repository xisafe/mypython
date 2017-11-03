import smtplib  
from email.mime.text import MIMEText
from email.mime import multipart
from email import encoders  
import os
mailto_list=['xuzhenhua@shanlinjinrong.com','443435766@163.com']  #收件人(列表)
def send_mail(to_list,sub,content,file_list=[]):
    mail_host="smtp.163.com"            #使用的邮箱的smtp服务器地址
    mail_user="job_shanlinjinrong@163.com"      #用户名
    mail_pass="Shanlinjinrong99"       #密码
    msg = multipart.MIMEMultipart()  
    msg['Subject'] = sub #邮件标题
    msg['From'] = mail_user
    print(msg)
    #msg['Cc'] = mail_user
    msg['To'] = ";".join(to_list)   #将收件人列表以‘；’分隔
    text_con = MIMEText(content,'plain',"utf-8")
    msg.attach(text_con)
    #att2 = MIMEText(open(file_list[0], 'rb').read(), 'base64', 'utf-8')
    #att2["Content-Type"] = 'application/octet-stream'
    #att2["Content-Disposition"] = 'attachment; filename="runoob.txt"'
    #msg.attach(att2)
    #if len(file_list)>0:
        #for attfile in file_list:
            #fname = os.path.basename(attfile)  
            #fp = open(attfile, 'rb')  
            #att =MIMEText(open(file_list[0], 'rb').read(), 'base64', 'utf-8')
            #att["Content-Type"] = 'application/octet-stream'  
            #att["Content-Disposition"] = 'attachment; filename="'+fname+'"'
            #att.add_header('Content-Disposition', 'attachment',filename=('gbk', '', basename))  
            #encoders.encode_base64(att)  
            #msg.attach(att)   
    try:
        server = smtplib.SMTP()
        server.connect(mail_host)     #连接服务器
        server.login(mail_user,mail_pass)    #登录操作
        server.sendmail(mail_user, to_list, msg.as_string())
        print('邮件发送成功')
        server.close()
        return True
    except Exception as e:
        print ('邮件发送失败：',str(e))
        return False
l=['E:/sljr/project/5-开发文档/Script/hive/log/2017-10-19/job_dictionary_group_03190190040.log',
   'E:/sljr/project/5-开发文档/Script/hive/log/2017-10-19/job_dim_area_03253825956.log']
send_mail(mailto_list,'ETL_错误','我去的赛场上',l)