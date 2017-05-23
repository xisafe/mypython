import math
import os
import pandas as pd
from sqlalchemy import create_engine
engine= create_engine('mssql+pymssql://publicezbuy:Yangqiang100%@192.168.199.106:1433/ezfinance',echo = True)
data = pd.read_sql_query('select top 1000 * from orders ',con= engine)
