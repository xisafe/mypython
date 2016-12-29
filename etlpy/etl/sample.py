
from base import RowSourceProxy, RowSource, Header, Field, FieldsCopier
import random

class Range:
    def __init__(self, start=0, count=None, step=1, stop=None):
        self.start = start 
        self.count = count 
        self.step = step 
        self.stop = stop

    def reset(self):
        self.rngidx = self.start
        self.returned = 0

    def next(self):
        if self.stop and self.rngidx >= self.stop or self.count and self.returned >= self.count:
            raise StopIteration()
        result = self.rngidx
        self.rngidx += self.step
        self.returned += 1
        return result

    def __iter__(self):
        return self

class Slicer(RowSourceProxy):
    def __init__(self, src, start=0, count=None, step=1, stop=None):
        RowSourceProxy.__init__(self, src)
        self.rng = Range(start, count, step, stop)

    def beforeFirstInputImpl(self):
        self.getNestedSource().beforeFirstInput()
        self.rng.reset()
        self.seqidx = 0

    def nextImpl(self):
        rngidx = self.rng.next()
        while True:
            row = self.getNestedSource().next()
            stop = self.seqidx >= rngidx
            self.seqidx += 1
            if stop:
                return row

class Sequence(RowSource):
    def __init__(self, name, start=1, count=None, step=1, stop=None):
        RowSource.__init__(self, Header([Field(name, type(start))]))
        self.rng = Range(start, count, step, stop)

    def openImpl(self):
        self.rng.reset()

    def nextImpl(self):
        return [self.rng.next()]

class Pickup(RowSource, FieldsCopier):
    def __init__(self, table, names = None, count=None):
        RowSource.__init__(self)
        self.table = table
        self.count = count
        self.names = names

    def openImpl(self):
        self.idx = 0
        names = self.names or self.table.getHeader().listNames()
        header = self.table.getHeader().cloneByNames(names)
        self.setHeader(header)
        self.makeHeaderPaths(self.table.getHeader(), self.getHeader())

    def nextImpl(self):
        if self.count and self.idx >= self.count:
            raise StopIteration()
        row = random.choice(self.table.getRows())
        self.idx += 1
        newrow = self.newBuffer()
        self.copyFields(row, newrow)
        return newrow
