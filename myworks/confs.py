import smtplib  
from email.mime.text import MIMEText
from email.mime import multipart
import os
local_path='E:/sljr/project/5-开发文档/Script/hive/'
main_path=local_path   
mailto_list=['xuzhenhua@shanlinjinrong.com']  #收件人(列表)
if not os.path.exists(local_path):
    main_path='/home/bigdata/'
    mailto_list=['wangxing@shanlinjinrong.com','liukuan@shanlinjinrong.com','wangting_sh@shanlinjinrong.com','yangjun_sh1@shanlinjinrong.com','daifahao@shanlinjinrong.com','xuzhenhua@shanlinjinrong.com','xuguoxian@shanlinjinrong.com','guanjiang@shanlinjinrong.com'] 
remote_path='/home/bigdata/'   #远程目录
main_path_bin=main_path+'bin/'  #本地的主目录路径下的bin
main_path_cfg=main_path+'cfg/'
main_path_sql=main_path+'sql/'
main_path_py=main_path+'python/'
remote_path_bin=remote_path+'bin/'
remote_path_cfg=remote_path+'cfg/'
remote_path_sql=remote_path+'sql/'
remote_path_py=remote_path+'python/'

sqoop_sh='sh /home/bigdata/bin/sqoop_handler_v1.1.ksh '
hive_sh='/home/bigdata/bin/hive_sql_handler_v1.0.ksh /home/bigdata/sql/'
db_map={                 "swzn_102":"coo",
                        "swdc_102":"dc_data_maintain",
                        "sljr":"hrdb",
                        "xfqz":"java_xingfuqianzhuang",
                        "jrxj":"sljr_jrxj",
                        "slsw":"sljr_slsw",
                        "bdgl":"sljr_borrow",
                        "csxt":"collection",
                        "yhcg":"sljr_bank",
                        "zhxt":"sljr_payment",
                        "lyxt":"sljr_pay_rout",
                        "fkxt":"sljr_risk",
                        "swzn":"coo",
                        "swdc":"dc_data_maintain"
                        }
etl_job_map={           "daily":"coo",
                        "swdc_102":"dc_data_maintain",
                        "sljr":"hrdb",
                        "xfqz":"java_xingfuqianzhuang",
                        "jrxj":"sljr_jrxj",
                        "slsw":"sljr_slsw",
                        "bdgl":"sljr_borrow",
                        "csxt":"collection",
                        "yhcg":"sljr_bank",
                        "zhxt":"sljr_payment",
                        "lyxt":"sljr_pay_rout",
                        "fkxt":"sljr_risk",
                        "swzn":"coo",
                        "swdc":"dc_data_maintain"
                        }
#mailto_list= ['xuzhenhua@shanlinjinrong.com','yangjun_sh1@shanlinjinrong.com']  #['wangxing@shanlinjinrong.com','liukuan@shanlinjinrong.com','wangting_sh@shanlinjinrong.com','yangjun_sh1@shanlinjinrong.com','daifahao@shanlinjinrong.com','xuzhenhua@shanlinjinrong.com','xuguoxian@shanlinjinrong.com','guanjiang@shanlinjinrong.com']
def send_mail(sub,content,filepath='',content_type='plain'):
    to_list=mailto_list
    mail_host="smtp.mxhichina.com"            #使用的邮箱的smtp服务器地址
    mail_user="bi_job@001bank.com"      #用户名
    mail_pass="Shanlinjinrong99"       #密码
    msg = multipart.MIMEMultipart()  
    msg['Subject'] = sub #邮件标题
    msg['From'] = mail_user
    msg['To'] = ";".join(to_list)   #将收件人列表以‘；’分隔
    if os.path.exists(filepath):
        fname = "attachment; filename="+os.path.basename(filepath)
        #print(fname)
        fs=open(filepath, 'r',encoding='utf-8')
        logs=fs.read()
        fs.close()
        att =MIMEText(logs, 'plain', 'utf-8')
        att["Content-Type"] = 'application/octet-stream'  
        att["Content-Disposition"] = fname
        msg.attach(att)
        content=content+'\n\n\n'+logs
        #print(content)
    else:
        print('')
    text_con = MIMEText(content,content_type,"utf-8") #plain html
    msg.attach(text_con)
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
