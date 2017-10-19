#!/usr/bin/env python3  
#coding: utf-8  
import smtplib  
from email.mime.text import MIMEText  
mailto_list=['xuzhenhua@cardvalue.cn','443435766@163.com']           #收件人(列表)
mail_host="smtp.cardvalue.cn"            #使用的邮箱的smtp服务器地址
mail_user="xuzhenhua@cardvalue.cn"                           #用户名
mail_pass="hua816813"                             #密码
mail_postfix="cardvalue.cn"                     #邮箱的后缀
def send_mail(to_list,sub,content):
    me="ERROR提醒"+"<"+mail_user+"@"+mail_postfix+">"
    msg = MIMEText(content,_subtype='plain')
    msg['Subject'] = sub
    msg['From'] = me
    msg['To'] = ";".join(to_list)                #将收件人列表以‘；’分隔
    try:
        server = smtplib.SMTP()
        server.connect(mail_host)                            #连接服务器
        server.login(mail_user,mail_pass)               #登录操作
        server.sendmail(me, to_list, msg.as_string())
        server.close()
        return True
    except Exception as e:
        print (str(e))
        return False
        