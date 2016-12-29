# -*- coding: cp1251 -*- 

import sys
sys.path.append("..")
from etl import *

import os

odbc = ODBCConnection("JIRA", "JIRA", "secret")

source = DBAPISource(odbc, "select * from WORKLOG order by ID")
target = CSVTarget("WORKLOG_new.csv", writeSpec=True)
FieldsPump(source, target)()

exists = os.access("WORKLOG.csv", os.F_OK)
if exists:
    src1 = CSVSource("WORKLOG.csv", readSpec=True)
else:
    tmp = CSVSource("WORKLOG_new.csv", readSpec=True)
    tmp.open()
    header = tmp.getHeader()
    tmp.close()
    src1 = RowSource(header)

diff=Diff(src1, CSVSource("WORKLOG_new.csv", readSpec=True), ["ID"])
FieldsPump(diff, CSVTarget("WORKLOG_diff.csv", writeSpec=True))()


if exists:
    os.unlink("WORKLOG.csv")
os.rename("WORKLOG_new.csv", "WORKLOG.csv")
