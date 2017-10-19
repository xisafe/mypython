from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
import pandas as pd
from pyhive import hive
from TCLIService.ttypes import TOperationState
#cursor = hive.connect('192.168.190.14').cursor()
#cursor.execute('SELECT * FROM my_awesome_data LIMIT 10', async=True)
# Presto
#engine = create_engine('presto://localhost:8080/hive/default')
# Hive
engine = create_engine('hive://bigdata@192.168.190.14:10000/dev')
print(engine)
dt=pd.read_sql('select * from dev.t1',engine)
#logs = Table('my_awesome_data', MetaData(bind=engine), autoload=True)
#print(select([func.count('*')], from_obj=logs).scalar())