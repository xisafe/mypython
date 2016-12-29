# -*- coding: cp1251 -*- 

import sys, os
sys.path.append("..")
from etl import *

srcs = []
for i in xrange(10):
    name = "id%d"%(i + 1) 
    src = Sequence(name, start=i+1, count=10)
    srcs.append(src)
FieldsPump(glue_sources(*srcs), OutputStreamTarget(sys.stdout))()
