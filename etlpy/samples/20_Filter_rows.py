# -*- coding: cp1251 -*- 

import sys
sys.path.append("..")
from etl import *

head1 = Header([Field("id"), Field("name"), Field("salary", float), ])
tab1 = MemoryTable(head1)

tab1.extend([
['1', 'Иvanov', 1000.0],
['1', 'Иvanov', 1000.0],
['2', 'Petrov', 1500.0],
['3', 'Sidorov', 2000.0],])

source=FilteredSource(MemoryTableSource(tab1), "src.salary>1000.0")
FieldsPump(source, OutputStreamTarget(sys.stdout, enc=('cp1251','cp866')))()
