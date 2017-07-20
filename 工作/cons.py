from sqlalchemy import create_engine
import pymongo
import psycopg2
import pandas as pd
#engine= create_engine('mssql+pymssql://publicezbuy:Yangqiang100%@192.168.199.106:1433/ezfinance',echo = True)
#data = pd.read_sql_query('select top 100000 * from orders ',con= engine)
def MSSQL106(db='ezfinance'):
    engine= create_engine(r'mssql+pymssql://publicezbuy:Aa123456!@192.168.199.106:1433/{0}'.format(db))
    #print(r'mssql+pymssql://publicezbuy:Aa123456!%@192.168.199.106:1433/{0}'.format(db))
    return engine;
def statis():
    engine= create_engine("mysql+pymysql://root:ezbuyisthebest@192.168.199.112:3306/statis?charset=utf8")
    return engine;
def db_ezbuy():
    engine= create_engine("mysql+pymysql://root:ezbuyisthebest@192.168.199.112:3306/statis?charset=utf8")
    return engine;
def mongo(db):
    client =pymongo.MongoClient('192.168.199.99',27017) # md.mongo_db14_rpt()
    return client[db]
def redshift():
    connenction_string = "dbname='dw' port='5439' user='root' password='Aa123456!' host='bi-uat-dw.chjy6qjn8rax.ap-southeast-1.redshift.amazonaws.com'";
    print("Connecting to \n        ->%s" % (connenction_string))
    conn = psycopg2.connect(connenction_string);
    return conn
def postgresql():
    engine= create_engine("postgresql+psycopg2://root:Aa123456!@bi-uat-dw.chjy6qjn8rax.ap-southeast-1.redshift.amazonaws.com:5439/dw")
    return engine;
if __name__ == '__main__':
    print("run this")
    #redshift()
    engine=redshift() #postgresql()
    sql1="""
    WbExport -type=text
         -delimiter=';'
         -header=true
         -file='/Users/hua/mytemp1.csv'
         -table=newtable;
    select * from dw.ic_cust limit 100"""
    #df = pd.read_sql_query(sql1,con= engine)
 