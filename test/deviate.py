# coding=gbk
#功能：计算输入值的偏离度
# encoding: gbk

import sys
import MySQLdb

obj = sys.argv[1]   # 要处理的参数对象 r,f,m,a
mcc = sys.argv[2]    # mcc
geo = sys.argv[3]   #地理位置类型 pro:省市 gps:gps坐标（百度）
province = sys.argv[4].decode("gbk") # 省份
city = sys.argv[5].decode("gbk") # 城市
lat = sys.argv[4] #纬度
lng = sys.argv[5] #经度
value = sys.argv[6] #参数对象的值
   
# print province; # 徐振华
def province_proc(obj, mcc, province,value):  # 按省进行计算
    try:
        #链接249
        
        conn=MySQLdb.connect(host='192.168.0.249',user='credit',passwd='Cvbaoli2015',db='cal_parm',charset="utf8",port=3306)
        #conn=MySQLdb.connect(host='127.0.0.1',user='root',passwd='',db='cal_parm',charset="utf8",port=3306) 
        cur=conn.cursor()
        
        #取得数量最大的前四条记录（第一条是均值avg记录，后三条为bin记录）
        #values = [mcc,province.decode('gbk').encode('utf-8'),obj] 
        values = [mcc,province,obj] 
        #print province
        cur.execute('SELECT `groupValue` FROM `cal_parm`.`cp_parm_group` WHERE mcc = %s AND province = %s AND city = "all" AND parmName = %s ORDER BY groupCount desc',values)
        # cur.execute('SELECT `groupValue` FROM `cal_parm`.`cp_parm_group` where province ="上海" ')
        record = cur.fetchmany(4) 

        min_dev = 9999; #最小偏离值：初始值给绝对大的值，不断比较找出最小值
        for r in record:
            
            dev = abs((float(value)-float(r[0]))/float(r[0]))
            
            if dev < min_dev:
               min_dev = dev
               
        cur.close()
        conn.close()
        
        return min_dev      
    
        
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        

# 中文编码问题
reload(sys)                         
sys.setdefaultencoding('utf-8')

# 把A当作R处理
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
      
