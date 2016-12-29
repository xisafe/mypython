# -*- coding: cp1251 -*- 

import sys
sys.path.append("..")
from etl import *

head1 = Header([Field("id"), Field("name"), ])
tab1 = MemoryTable(head1)

tab1.extend([
['1', 'Иvanov'],
['1', 'Иvanov'],
['1', 'Иvanov'],
['2', 'Petrov'],
['3', 'Sidorov'],])

head2 = Header([Field("id"), Field("salary", float), ])
tab2 = MemoryTable(head2)
tab2.extend([
['1', 1000.0],
['3', 3000.0],
['3', 3500.0],
['4', 2000.0],
['5', 4000.0],
])

source=LookupSource(MemoryTableSource(tab1), MemoryTableLookup(["id"], tab2), 
    passAll=True, hitCountCol="HIT_COUNT")
FieldsPump(source, OutputStreamTarget(sys.stdout, enc=('cp1251','cp866')))()
