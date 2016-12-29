# -*- coding: cp1251 -*- 

import sys, os
sys.path.append("..")
from etl import *

tab = MemoryTable(Header(
    [Field("id"), Field("name"),]), [
    ['3', 'Sidorov'],
    ['2', 'Petrov'],
    ['4', 'Vasechkin'],
    ['3', 'Sidorov'],
    ['1', 'Иvanov'],
])

source = Slicer(MemoryTableSource(tab), step=2, count=2)
FieldsPump(source, OutputStreamTarget(sys.stdout))()
