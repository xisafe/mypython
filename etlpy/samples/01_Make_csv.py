# -*- coding: cp1251 -*- 

import sys
sys.path.append("..")
from etl import *

tab = MemoryTable(Header(
    [("id", str), ("name", str), ("salary", float)]), [
    ['3', 'Sidorov', 3000.0],
    ['2', 'Petrov', 2000.0],
    ['4', 'Vasechkin', 4000.0],
    ['3', 'Sidorov', 3000.0],
    ['1', 'Иvanov', 1000.0],
])


FieldsPump(MemoryTableSource(tab), CSVTarget("emp.csv", writeSpec=True))()
