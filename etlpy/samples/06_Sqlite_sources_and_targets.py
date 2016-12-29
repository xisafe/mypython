# -*- coding: cp1251 -*- 

import sys
sys.path.append("..")
from etl import *

import os
try:
    os.remove("test.db")
except:
    pass

sqlite = SQLiteConnection("test.db")
sqlite.open()
conn = sqlite.getConnection()
conn.execute('''create table stocks(date date, trans text, symbol text,qty numeric(10), price real, photo BLOB)''')
for t in (('2006-03-28', 'BUY', 'IBM', 1000, 45.00, '123'),
    ('2006-04-05', 'BUY', 'MSOFT', 1000, 72.00, '123'),
    ('2006-04-06', 'SELL', 'Неофлекс', 500, 53.00, 'фото'),
    ):
    conn.execute('insert into stocks values (?,?,?,?,?,?)', t)
sqlite.close()

source = TableSource(sqlite, "stocks")
target = CSVTarget("dbapi.csv", writeSpec=True)
FieldsPump(source, target)()

sqlite.open()
sqlite.getConnection().execute('''create table stocks2(date date, trans text, symbol text,qty numeric(10), price real, photo BLOB)''')
sqlite.close()

source = CSVSource("dbapi.csv", readSpec=True)
target = TableTarget(sqlite, "stocks2")
FieldsPump(source, target)()

source = CSVSource("dbapi.csv", readSpec=True)
source = Slicer(source, start=0, count=1)
target = TableTarget(sqlite, "stocks2", target_type="U", keys=["date", "trans", "symbol"])
FieldsPump(source, target)()

source = CSVSource("dbapi.csv", readSpec=True)
source = Slicer(source, start=2, count=1)
target = TableTarget(sqlite, "stocks2", target_type="D", keys=["date", "trans", "symbol"])
FieldsPump(source, target)()

FieldsPump(DBAPISource(sqlite, "select * from stocks2 order by price"), OutputStreamTarget(sys.stdout, enc=('cp1251','cp866')))()
