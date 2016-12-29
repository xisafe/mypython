# -*- coding: cp1251 -*- 

import sys
sys.path.append("..")
from etl import *

head1 = Header([Field("id", str, 10), Field("name", str, 60), Field("salary", float), ])
tab1 = MemoryTable(head1)

tab1.extend([
['1', 'Иvanov', 1000.0],
['2', 'Petrov', 1500.0],
['3', 'Sidorov', 2000.0],])

tab2 = MemoryTable(head1)
tab2.extend([
['1', 'Иvanov', 1000.0],
['3', 'Sidorov', 3000.0],
#['5', 'Sidorov', 3000.0],
['4', 'Орлов', 2000.0],
])

diff=Diff(MemoryTableSource(tab1), MemoryTableSource(tab2), ["id"])
FieldsPump(diff, OutputStreamTarget(sys.stdout, enc=('cp1251','cp866')))()
