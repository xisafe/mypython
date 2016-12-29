import sys
sys.path.append("..")
from etl import *

import locale
locale.setlocale(locale.LC_ALL, 'ru')

src = TimeDimSource(datetime.date(2000, 1, 1), 100)
trg = XBaseTarget("TIMEDIM.DBF")
FieldsPump(src, trg)()
