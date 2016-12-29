# coding=gbk
#���ܣ���������ֵ��ƫ���
# encoding: gbk

import sys
import MySQLdb

obj = sys.argv[1]   # Ҫ����Ĳ������� r,f,m,a
mcc = sys.argv[2]    # mcc
geo = sys.argv[3]   #����λ������ pro:ʡ�� gps:gps���꣨�ٶȣ�
province = sys.argv[4].decode("gbk") # ʡ��
city = sys.argv[5].decode("gbk") # ����
lat = sys.argv[4] #γ��
lng = sys.argv[5] #����
value = sys.argv[6] #���������ֵ
   
# print province; # ����
def province_proc(obj, mcc, province,value):  # ��ʡ���м���
    try:
        #����249
        
        conn=MySQLdb.connect(host='192.168.0.249',user='credit',passwd='Cvbaoli2015',db='cal_parm',charset="utf8",port=3306)
        #conn=MySQLdb.connect(host='127.0.0.1',user='root',passwd='',db='cal_parm',charset="utf8",port=3306) 
        cur=conn.cursor()
        
        #ȡ����������ǰ������¼����һ���Ǿ�ֵavg��¼��������Ϊbin��¼��
        #values = [mcc,province.decode('gbk').encode('utf-8'),obj] 
        values = [mcc,province,obj] 
        #print province
        cur.execute('SELECT `groupValue` FROM `cal_parm`.`cp_parm_group` WHERE mcc = %s AND province = %s AND city = "all" AND parmName = %s ORDER BY groupCount desc',values)
        # cur.execute('SELECT `groupValue` FROM `cal_parm`.`cp_parm_group` where province ="�Ϻ�" ')
        record = cur.fetchmany(4) 

        min_dev = 9999; #��Сƫ��ֵ����ʼֵ�����Դ��ֵ�����ϱȽ��ҳ���Сֵ
        for r in record:
            
            dev = abs((float(value)-float(r[0]))/float(r[0]))
            
            if dev < min_dev:
               min_dev = dev
               
        cur.close()
        conn.close()
        
        return min_dev      
    
        
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        

# ���ı�������
reload(sys)                         
sys.setdefaultencoding('utf-8')

# ��A����R����
if obj == 'a':
   obj = 'r'
   
if obj not in ('r','f','m','a'):
    print '99-wrong object'
    exit()

if geo.startswith('pro'):
  rv = province_proc(obj,mcc,province,value)
  print rv
elif geo.startswith('gps'):
  print '99-dont support gps'
else:
  print '99-wrong geo type' 
      
