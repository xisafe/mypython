import MySQLdb
m='201504'
lastMonth = int(m)
monthCnt = 3
input=[4345,5467,4845]
try:
    conn=MySQLdb.connect(host='192.168.0.249',user='credit',passwd='Cvbaoli2015',db='cal_parm',charset="utf8",port=3306)
    cur=conn.cursor()
    monthNo = int(m[-2:])
    standard =[]
    for j in range(0, monthCnt):
        values =[1520,monthNo]
        cur.execute('SELECT `avgAmouont` FROM `cal_parm`.`cp_12month_amount` WHERE mcc = %s  AND monthNo = %s', values)
        result=cur.fetchone()
        valuess =["5099"]
        cur.execute('SELECT `groupValue` FROM `cal_parm`.`cp_parm_group` WHERE mcc =%s AND city = "all" AND parmName = "r" ORDER BY groupCount desc',valuess)
        record = cur.fetchmany(4)
        print record
        standard.append(result[0])
        monthNo = monthNo - 1
        if monthNo == 0: monthNo=12
       # print standard, monthNo
except MySQLdb.Error,e:
    print "Mysql Error %d: %s"