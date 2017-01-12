#!/usr/bin/env python3  
#coding: utf-8  
import smtplib  
from email.mime.text import MIMEText  
  
sender = '443435766@163.com'  
receiver = '443435766@163.com'  
subject = 'husdsgf你好'  
smtpserver = 'smtp.163.com'  
username = '443435766'  
password = 'hua18221007242'  
msg = MIMEText('<html><h1>你好</h1></html>','html','utf-8')  
msg['Subject'] = subject  
smtp = smtplib.SMTP()  
smtp.connect('smtp.163.com')  
smtp.login(username, password)  
smtp.sendmail(sender, receiver, msg.as_string())  
smtp.quit()  