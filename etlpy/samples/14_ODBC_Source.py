# -*- coding: cp1251 -*- 

import sys
sys.path.append("..")
from etl import *

odbc = ODBCConnection("JIRA", "JIRA", "secret")
source = TableSource(odbc, "WORKLOG")
target = CSVTarget("WORKLOG.csv", writeSpec=True)
FieldsPump(source, target)()

