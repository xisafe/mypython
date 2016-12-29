
import base, csv, types

class CSVTarget(base.RowTarget):
    def __init__(self, name, header = None, writeHeader = True, writeSpec=False, delimiter=";", quoting=csv.QUOTE_MINIMAL):
        base.RowTarget.__init__(self, header)
        self.name = name
        self.writeHeader = writeHeader
        self.delimiter = delimiter
        self.quoting = quoting
        self.writeSpec = writeSpec

    def beforeFirstOutputImpl(self):
        if self.writeHeader:
            self.writer.writerow(self.getHeader().listNames())
        if self.writeSpec:
            self.writer.writerow([repr(s) for s in self.getHeader().listSpecs()])

    def writerowImpl(self, row):
        self.writer.writerow(row)

    def openImpl(self):
        self.file = open(self.name, 'wb')
        self.writer = csv.writer(self.file, delimiter=self.delimiter, quoting=self.quoting)

    def closeImpl(self):
        self.file.close()
        
class CSVSource(base.RowSource):
    def __init__(self, name, header = None, readHeader = True, delimiter=";", readSpec=False):
        base.RowSource.__init__(self, header)
        self.name = name
        self.readHeader = readHeader
        self.delimiter = delimiter
        self.readSpec = readSpec

    def openImpl(self):
        self.file = open(self.name, 'rb')
        self.reader = csv.reader(self.file, delimiter=self.delimiter)
        if self.readHeader:
            self.setHeader(base.Header(names=self.reader.next()))
        if self.readSpec:
            specs = self.reader.next()
            if specs:
                header = self.getHeader()
                if header:
                    specs = [eval(s) for s in specs]
                    self.convs = []
                    for i, s in enumerate(specs):
                        header[i].type = types.registry.getTypeByName(s[0])
                        header[i].length = s[1]
                        header[i].prec = s[2]
                        self.convs.append(types.registry.getConverterByTypes(str, header[i].type))

    def closeImpl(self):
        self.file.close()

    def nextImpl(self):
        row = self.reader.next()
        if self.readSpec:
            for i, value in enumerate(row):
                row[i] = value and self.convs[i](value) or None
        return row
