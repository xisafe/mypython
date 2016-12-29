# -*- coding: cp1251 -*- 

import sys
sys.path.append("..")
from etl import *

tab1 = MemoryTable(Header(
    [("id"), ("name"),]), [
    ['3', 'Sidorov'],
    ['2', 'Petrov'],
    ['4', 'Vasechkin'],
    ['3', 'Sidorov'],
    ['1', 'Иvanov'],
])

def getCount(ctx):
    ctx.cnt = (ctx.cnt or 0) + 1
    return ctx.cnt

cs=ChangeStructure(MemoryTableSource(tab1), newfields=[
    ("salary", "str(int(src.id)*10)"),
    (("cnt", int), getCount),
])
FieldsPump(cs, OutputStreamTarget(sys.stdout, enc=('cp1251','cp866')))()
