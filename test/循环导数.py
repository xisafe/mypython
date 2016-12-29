#encoding=utf-8
import pymssql
server ="192.168.0.98\SQLSERVERDB"
user = "sa"
password = "!@#qweasdZXC"
conn = pymssql.connect(server, user, password, "cv_dw")
dict={0:"v0",1:"v1",2:"v2",3:"v3",4:"v4",5:"v5",6:"v6",7:"v7",8:"v8",9:"v9",10:"v10"}
cursor=conn.cursor()
for i in dict:
    sql="insert into temp_Corr_loan select DATEADD(day, %d, AssessDate) OperDate,'%s' Dtype from t_fact_Status_FirstCheck where AssessDate>'2015-01-01'"%(i,dict[i])
    print sql
    cursor.execute(sql)
    conn.commit()
    #row = cursor.fetchone()
    #while row:
       #print("Name=%s" % (row[1]))
       #row = cursor.fetchone()
conn.close()
