import base, fileetl
import datetime, calendar

class TimeDimSource(base.RowSource):
    def __init__(self, start, total):
        base.RowSource.__init__(self)
        header = base.Header(fields=[
            base.Field("DATE_NO", int, 8),
            base.Field("DATE_DT", datetime.date, 8),
            base.Field("DATE_CD", str, 10),
            base.Field("DAY_NO", int, 2),
            base.Field("MONTH_NO", int, 2),
            base.Field("MONTH_NM", str, 12),
            base.Field("MONTH_SN", str, 3),
            base.Field("YEAR_NO", int, 4),
            base.Field("LEAP_LFG", bool, 1),
            base.Field("WEEKDAY_NO", int, 1),
            base.Field("WEEKDAY_NM", str, 12),
            base.Field("WEEKDAY_SN", str, 3),
            base.Field("QUORTER_NO", int, 1),
            base.Field("QUORTER_CD", str, 2),
            base.Field("WEEK_NO", int, 2),
            base.Field("DOFY_NO", int, 3),
            base.Field("YM_NO", int, 6),
            base.Field("YM_CD", str, 8),
            base.Field("YQ_NO", int, 5),
            base.Field("YQ_CD", str, 7),
        ])
        self.setHeader(header)
        self.start = start
        self.total = total

    def generate(self):
        for shift in xrange(self.total):
            current = self.start + datetime.timedelta(shift)
            result = []
            result.append(current.day + current.month*100 + current.year*10000)
            result.append(current) 
            result.append(current.isoformat())
            result.append(current.day )
            result.append(current.month)
            result.append(calendar.month_name[current.month])
            result.append(calendar.month_abbr[current.month])
            result.append(current.year)
            result.append(calendar.isleap(current.year))
            weekday = current.weekday() 
            result.append(weekday + 1)
            result.append(calendar.day_name[weekday])
            result.append(calendar.day_abbr[weekday])
            quorter = int(current.month/4) + 1
            result.append(quorter)
            result.append("Q%d"%quorter)
            weekno = current.isocalendar()[1]
            result.append(weekno)
            ny = datetime.date(current.year, 1, 1)
            result.append((current - ny).days + 1)
            result.append(current.year*100 + current.month)
            result.append("Y%04dM%02d"%(current.year, current.month))
            result.append(current.year*10 + quorter)
            result.append("Y%04dQ%01d"%(current.year, quorter))
            yield result

    def openImpl(self):
        self.generator = self.generate()

    def nextImpl(self):
        return self.generator.next()


if __name__ == '__main__':
    import sys, locale, codecs
    locale.setlocale(locale.LC_ALL, 'ru')
    src = TimeDimSource(datetime.date(2000, 2, 1), 10)
    trg = fileetl.OutputStreamTarget( codecs.EncodedFile(sys.stdout, 'cp1251', 'cp866', 'ignore') )
    base.FieldsPump(src, trg)()
