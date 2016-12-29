#功能：计算商户月交易额的相关性
#coding=utf-8

import MySQLdb
import sys
import numpy  as ny
import pandas as pd

#依次获取输入参数: 
# 1）mcc码 : 4位数字
# 2）省份名：中文，无‘省’字。 如：湖北
# 3) 上一个交易月份： yyyymm  如：201504
# 4）连续交易月数：数字，必须>=3
# 5)~?) 依次为从上个月开始的每个月的交易额（由近到远），具体个数由“4）连续交易月数”决定
mcc = sys.argv[1]
#province = sys.argv[2].decode('gb2312').encode('utf8')
province = sys.argv[2]
try:
  lastMonth = int(sys.argv[3])  # 上一交易月
  monthCnt =  int(sys.argv[4])  # 连续交易月数
  if  monthCnt <3:
      print '<3 moth'

      #return '90-less than 3 moth' 
  
  #取得各月的交易量的list
  input =[]    
  for i in range(0, monthCnt):
      input.append(float(sys.argv[5+i]))
  #print input
except ValueError:
  print "error"
  #return '99-input not numeric'
       
try:
    #链接249
    conn=MySQLdb.connect(host='192.168.0.249',user='credit',passwd='Cvbaoli2015',db='cal_parm',charset="utf8",port=3306)
    #conn=MySQLdb.connect(host='127.0.0.1',user='root',passwd='',db='cal_parm',charset="utf8",port=3306) 
    cur=conn.cursor()      
    
    #取得上一交易月的月份数
    monthNo = int(sys.argv[3][-2:]) 
    
    #取得同地区同MCC各月的交易量的list
    standard =[] 
    for j in range(0, monthCnt):
        values =[mcc,province,monthNo]
        #print values
               
        cur.execute('SELECT `avgAmouont` FROM `cal_parm`.`cp_12month_amount` WHERE mcc = %s AND province =%s AND monthNo = %s', values)
        
        result=cur.fetchone()
        standard.append(result[0])
        
        #计算再往上一个月的月份数，遇到0即为上一年底
        monthNo = monthNo - 1
        if monthNo == 0:
           monthNo = 12
    #print standard

    #计算相关系数
    cmp =[input,standard]    
    r = ny.corrcoef(cmp)
    print r[0][1]
    

    
except MySQLdb.Error,e:
    print "Mysql Error %d: %s" % (e.args[0], e.args[1])
