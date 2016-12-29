
import base
import xbase
import datetime
import decimal

def decodeType(type_):
    if type_ in [int, float, decimal.Decimal]:
        return 'N'
    elif type_ == type(True):
        return 'L'
    elif type_ in [datetime.date, datetime.datetime]:
        return 'D'
    else:
        return 'C'

class XBaseTarget(base.RowTarget):
    def __init__(self, name, header = None):
        base.RowTarget.__init__(self, header)
        self.name = name

    def writerowImpl(self, row):
        self.records.append(row)

    def openImpl(self):
        self.records = []

    def closeImpl(self):
        output = open(self.name, 'wb')
        xbase.dbfwriter(output, self.getHeader().listNames(), self.getFieldspecs(), self.records)
        output.close()
        del self.records
        
    def getFieldspecs(self):
        result = []
        header = self.getHeader()
        for i in xrange(len(header)):
            field = header[i]
            result.append((decodeType(field.getType()), field.getLength(), field.getPrec()))
        return result

def encodeType(type_, prec):
    if type_ == 'N':
        if prec > 0:
            return decimal.Decimal
        else:
            return type(0)
    elif type_ == 'D':
        return datetime.date
    else: 
        return type('')

class XBaseSource(base.RowSource):
    def __init__(self, name):
        base.RowSource.__init__(self)
        self.name = name

    def openImpl(self):
        self.input = open(self.name, 'rb')
        self.reader = xbase.dbfreader(self.input)
        names = self.reader.next()
        specs = self.reader.next()
        header = base.Header()
        for i, name in enumerate(names):
            type, length, prec = specs[i]
            header.append(base.Field(name, type=encodeType(type, prec), length=length, prec=prec))
        self.setHeader(header)

    def closeImpl(self):
        self.input.close()

    def nextImpl(self):
        return self.reader.next()

