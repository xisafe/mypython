# encoding: utf-8
from os import getenv
import pymssql

server ="192.168.0.98\SQLSERVERDB"
user = "reader"
password = "reader"
conn = pymssql.connect(server, user, password, "cv_dw")
cursor = conn.cursor()
sql="SELECT lead_id,BusinessName FROM Dim_leads WHERE BusinessName like '%s'"%'江西%'
print sql

cursor.execute(sql)
#cursor.execute("SELECT lead_id,BusinessName FROM Dim_leads WHERE BusinessName like '%s'", sql)
#cursor.execute("SELECT lead_id,BusinessName FROM Dim_leads WHERE BusinessName like '江西%'")
#for row in cursor:
  #  print('row = %r'%(row,)).decode("utf-8")
row = cursor.fetchone()
while row:
    print("ID=%d, Name=%s" % (row[0], row[1]))
    row = cursor.fetchone()
conn.close()