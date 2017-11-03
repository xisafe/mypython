import smtplib  
from email.mime.text import MIMEText
from email.mime import multipart
import os
mailto_list=['xuzhenhua@shanlinjinrong.com','443435766@163.com']  #收件人(列表)
def send_mail(to_list,sub,content,filepath=''):
    mail_host="smtp.163.com"            #使用的邮箱的smtp服务器地址
    mail_user="job_shanlinjinrong@163.com"      #用户名
    mail_pass="Shanlinjinrong99"       #密码
    msg = multipart.MIMEMultipart()  
    msg['Subject'] = sub #邮件标题
    msg['From'] = mail_user
    msg['To'] = ";".join(to_list)   #将收件人列表以‘；’分隔
    if os.path.exists(filepath):
        fname = "attachment; filename="+os.path.basename(filepath)
        print(fname)
        fs=open(filepath, 'r',encoding='utf-8')
        logs=fs.read()
        fs.close()
        att =MIMEText(logs, 'plain', 'utf-8')
        att["Content-Type"] = 'application/octet-stream'  
        att["Content-Disposition"] = fname
        msg.attach(att)
        content=content+'\n\n\n'+logs
        print(content)
    else:
        pass
    text_con = MIMEText(content,'plain',"utf-8")
    msg.attach(text_con)
    try:
        server = smtplib.SMTP()
        server.connect(mail_host)     #连接服务器
        server.login(mail_user,mail_pass)    #登录操作
        server.sendmail(mail_user, to_list, msg.as_string())
        #print('邮件发送成功')
        server.close()
        return True
    except Exception as e:
        print ('邮件发送失败：',str(e))
        return False

send_mail(mailto_list,'设计师','我去的赛场上' )