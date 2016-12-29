# -*- coding: cp1251 -*- 

import sys
sys.path.append("..")
from etl import *

tab1 = MemoryTable(Header(
    ["id", "name"]), [
    ['3', 'Sidorov'],
    ['2', 'Petrov'],
    ['4', 'Vasechkin'],
    ['3', 'Sidorov'],
    ['1', 'Иvanov'],
])

source=Pickup(tab1, ["name"], count=10)
FieldsPump(source, OutputStreamTarget(sys.stdout, enc=('cp1251','cp866')))()
