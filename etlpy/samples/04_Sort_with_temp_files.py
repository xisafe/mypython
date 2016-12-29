# -*- coding: cp1251 -*- 
# encoding:utf-8
import sys
sys.path.append("..")
from etl import *

tab = MemoryTable(Header(
    [Field("id"), Field("name"),]), [
    ['3', 'Sidorov'],
    ['2', 'Petrov'],
    ['4', 'Vasechkin'],
    ['3', 'Sidorov'],
    ['1', 'Hvanov'],
])

source=Sort(MemoryTableSource(tab), ["id"], maxrows=2) # 2 temp files created
FieldsPump(source, OutputStreamTarget(sys.stdout, enc=('cp1251','cp866')))()
