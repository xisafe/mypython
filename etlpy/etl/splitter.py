
from base import RowTarget, callableFromString
from structure import Context, Row

class SplittedTarget(RowTarget):
    def __init__(self, header = None, targets = None):
        RowTarget.__init__(self, header)
        self.targets = []
        self.filters = []
        self.stop = []
        for trg in (targets or []):
            if isinstance(trg, list):
                trg = tuple(trg)
            if not isinstance(trg, tuple):
                trg = (trg,)
            self.addTarget(*trg)

    def addTarget(self, trg, flt=None, stop=False):
        self.targets.append(trg)
        if flt != None and not callable(flt):
            flt = callableFromString(str(flt))
        self.filters.append(flt)
        self.stop.append(stop)

    def setHeaderImpl(self, header, i=None):
        if i == None:
            RowTarget.setHeaderImpl(self, header)
            for i in xrange(len(self.targets)):
                self.setHeaderImpl(header, i)
        else:
            self.targets[i].setHeader(header)

    def flushImpl(self):
        RowTarget.flushImpl(self)
        for trg in self.targets:
            trg.flush()

    def openImpl(self):
        for trg in self.targets:
            trg.open()
        self.srcRow = Row(self.getSource().getHeader())
        self.ctx = Context()
        self.ctx["src"] = self.srcRow

    def closeImpl(self):
        for trg in self.targets:
            trg.close()

    def writerowImpl(self, row):
        self.srcRow.setCurrent(row)
        for i, trg in enumerate(self.targets):
            if self.filters[i] == None or bool(self.filters[i](self.ctx)):
                trg.writerow(row)
                trg.afterWriterow()
                if self.stop[i]:
                    break

    def beforeFirstOutputImpl(self):
        RowTarget.beforeFirstOutputImpl(self)
        for trg in self.targets:
            trg.beforeFirstOutput()

    def afterLastOutputImpl(self):
        RowTarget.afterLastOutputImpl(self)
        for trg in self.targets:
            trg.afterLastOutput()

    def connectSourceImpl(self, source):
        RowTarget.connectSourceImpl(self, source)
        for trg in self.targets:
            trg.connectSource(source)
