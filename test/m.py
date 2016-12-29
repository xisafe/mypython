# encoding: utf-8
# This Python file uses the following encoding: utf-8
from pandas import DataFrame #从pandas库中引用DataFrame
import pymssql as mssql
import sqlalchemy as s
from sqlalchemy.orm import sessionmaker
server = "192.168.0.98\SQLSERVERDB"
user = "sa"
password = "!@#qweasdZXC"
eng =s.create_engine("mssql+pymssql://sa:!@#qweasdZXC@192.168.0.98\\SQLSERVERDB/cv_dw?charset=utf8",deprecate_large_types=True)
conn = eng.connect()  #建立连接
# 4. 查询表信息
result = conn.execute("select userName name from Dim_Users")
Session = sessionmaker(bind=eng)
session = Session()
print session.execute('select * from Dim_Users').fetchall()
#for row in result:
   # print "name: ", row['name']
# 5. 关闭连接
conn.close()