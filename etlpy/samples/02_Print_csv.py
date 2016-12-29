# -*- coding: cp1251 -*- 

import sys
sys.path.append("..")
from etl import *

FieldsPump(CSVSource("emp.csv", readSpec=True), OutputStreamTarget(sys.stdout, enc=('cp1251','cp866')))()
